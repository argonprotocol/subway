use jsonrpsee::server::{RpcModule, ServerBuilder, SubscriptionCloseResponse};
use serde_json::json;

use crate::{
    config::{Config, MergeStrategy, MiddlewaresConfig, RpcDefinitions, RpcSubscription},
    extensions::{
        client::{
            mock::{SinkTask, TestServerBuilder},
            Client, ClientConfig,
        },
        merge_subscription::MergeSubscriptionConfig,
        server::ServerConfig,
        ExtensionsConfig,
    },
    server,
};

fn config(endpoints: Vec<String>, subscriptions: Vec<RpcSubscription>) -> Config {
    Config {
        extensions: ExtensionsConfig {
            client: Some(ClientConfig {
                endpoints,
                shuffle_endpoints: false,
                request_timeout_seconds: None,
                connection_timeout_seconds: None,
                retries: None,
            }),
            server: Some(ServerConfig {
                listen_address: "0.0.0.0".to_string(),
                port: 0,
                max_connections: 10,
                max_subscriptions_per_connection: 1024,
                max_batch_size: None,
                request_timeout_seconds: 120,
                http_methods: Vec::new(),
                cors: None,
            }),
            merge_subscription: Some(MergeSubscriptionConfig {
                keep_alive_seconds: Some(1),
            }),
            ..Default::default()
        },
        middlewares: MiddlewaresConfig {
            methods: vec![],
            subscriptions: vec!["merge_subscription".to_string(), "upstream".to_string()],
        },
        rpcs: RpcDefinitions {
            methods: vec![],
            subscriptions,
            aliases: vec![],
        },
    }
}

#[tokio::test]
async fn merge_subscription_works() {
    let subscribe_head = "chain_subscribeNewHeads";
    let update_head = "chain_newHead";
    let unsubscribe_head = "chain_unsubscribeNewHeads";

    let subscribe_finalized = "chain_subscribeFinalizedHeads";
    let update_finalized = "chain_finalizedHead";
    let unsubscribe_finalized = "chain_unsubscribeFinalizedHeads";

    let subscribe_mock = "mock_sub";
    let unsubscribe_mock = "mock_unsub";
    let update_mock = "mock";
    let params = vec![json!(["0x01"])];

    let mut builder = TestServerBuilder::new();

    let mut head_sub = builder.register_subscription(subscribe_head, update_head, unsubscribe_head);
    let mut finalized_sub = builder.register_subscription(subscribe_finalized, update_finalized, unsubscribe_finalized);
    let mut mock_sub_rx = builder.register_subscription(subscribe_mock, update_mock, unsubscribe_mock);

    let (addr, _upstream_handle) = builder.build().await;

    tokio::spawn(async move {
        let head_sub = head_sub.recv().await.unwrap();
        let finalized_sub = finalized_sub.recv().await.unwrap();

        head_sub.run_sink_tasks(vec![SinkTask::Send(json!(1))]).await;
        finalized_sub.run_sink_tasks(vec![SinkTask::Send(json!(1))]).await;
    });

    let config = config(
        vec![format!("ws://{addr}")],
        vec![
            RpcSubscription {
                subscribe: subscribe_head.to_string(),
                unsubscribe: unsubscribe_head.to_string(),
                name: update_head.to_string(),
                merge_strategy: None,
            },
            RpcSubscription {
                subscribe: subscribe_finalized.to_string(),
                unsubscribe: unsubscribe_finalized.to_string(),
                name: update_finalized.to_string(),
                merge_strategy: None,
            },
            RpcSubscription {
                subscribe: subscribe_mock.to_string(),
                unsubscribe: unsubscribe_mock.to_string(),
                name: update_mock.to_string(),
                merge_strategy: Some(MergeStrategy::MergeStorageChanges),
            },
        ],
    );

    let subway_server = server::build(config).await.unwrap();
    let addr = subway_server.addr;

    let client = Client::with_endpoints([format!("ws://{addr}")]).unwrap();
    let mut first_sub = client
        .subscribe(subscribe_mock, params.clone(), unsubscribe_mock)
        .await
        .unwrap();

    let send_msg = tokio::spawn(async move {
        let sub = mock_sub_rx.recv().await.unwrap();

        sub.run_sink_tasks(vec![
            SinkTask::Send(json!({
                "block": "0x01",
                "changes": [
                    ["0x01", "hello"],
                    ["0x02", null]
                ]
            })),
            SinkTask::Sleep(100),
            SinkTask::Send(json!({
                "block": "0x02",
                "changes": [
                    ["0x02", "world"]
                ]
            })),
            SinkTask::Sleep(100),
            SinkTask::Send(json!({
                "block": "0x03",
                "changes": [
                    ["0x01", null],
                    ["0x02", "bye"]
                ]
            })),
            SinkTask::Sleep(100),
            SinkTask::Send(json!({
                "block": "0x04",
                "changes": [
                    ["0x01", "hello"],
                    ["0x02", "again"]
                ]
            })),
            // after 1s upstream subscription is dropped
            SinkTask::SinkClosed(Some(1)),
        ])
        .await;
    });

    let test_one = tokio::spawn(async move {
        assert_eq!(
            first_sub.next().await.unwrap().unwrap(),
            json!({
                "block": "0x01",
                "changes": [
                    ["0x01", "hello"],
                    ["0x02", null]
                ]
            })
        );

        assert_eq!(
            first_sub.next().await.unwrap().unwrap(),
            json!({
                "block": "0x02",
                "changes": [
                    ["0x02", "world"],
                ]
            })
        );

        assert_eq!(
            first_sub.next().await.unwrap().unwrap(),
            json!({
                "block": "0x03",
                "changes": [
                    ["0x01", null],
                    ["0x02", "bye"]
                ]
            })
        );

        // first subscription will unsubscribe but it shouldn't affect second subscription
        first_sub.unsubscribe().await.unwrap();
    });

    // second subscription happens after 2nd msg is send (100ms) and 3rd msg (200ms)
    // so 1st msg for the second subscription will be a merge between 1st & 2nd msg ["block": "0x02"]
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let mut second_sub = client
        .subscribe(subscribe_mock, params, unsubscribe_mock)
        .await
        .unwrap();

    let test_two = tokio::spawn(async move {
        // 2nd msg with merged storage changes
        assert_eq!(
            second_sub.next().await.unwrap().unwrap(),
            json!({
                "block": "0x02",
                "changes": [
                    ["0x01", "hello"],
                    ["0x02", "world"],

                ]
            })
        );

        // 3rd msg is the same as the first subscription is getting
        assert_eq!(
            second_sub.next().await.unwrap().unwrap(),
            json!({
                "block": "0x03",
                "changes": [
                    ["0x01", null],
                    ["0x02", "bye"]
                ]
            })
        );

        // got 4th msg
        assert_eq!(
            second_sub.next().await.unwrap().unwrap(),
            json!({
                "block": "0x04",
                "changes": [
                    ["0x01", "hello"],
                    ["0x02", "again"]
                ]
            })
        );

        second_sub.unsubscribe().await.unwrap();
    });

    send_msg.await.unwrap();
    test_one.await.unwrap();
    test_two.await.unwrap();

    // stop server
    subway_server.handle.stop().unwrap();
}

#[tokio::test]
async fn multi_key_storage_subscriptions_receive_initial_values_and_updates() {
    let subscribe = "state_subscribeStorage";
    let update = "state_storage";
    let unsubscribe = "state_unsubscribeStorage";
    let params = vec![json!(["0x01", "0x02"])];

    let mut builder = TestServerBuilder::new();
    let mut upstream_subscriptions = builder.register_subscription(subscribe, update, unsubscribe);
    let (addr, _upstream_handle) = builder.build().await;

    tokio::spawn(async move {
        let first = upstream_subscriptions.recv().await.unwrap();
        first
            .send(json!({
                "block": "0x01",
                "changes": [["0x01", "first"], ["0x02", null]]
            }))
            .await;

        let second = upstream_subscriptions.recv().await.unwrap();
        second
            .send(json!({
                "block": "0x02",
                "changes": [["0x01", "second"], ["0x02", null]]
            }))
            .await;

        first
            .send(json!({
                "block": "0x03",
                "changes": [["0x02", "first-update"]]
            }))
            .await;
        second
            .send(json!({
                "block": "0x03",
                "changes": [["0x02", "second-update"]]
            }))
            .await;
    });

    let config = config(
        vec![format!("ws://{addr}")],
        vec![RpcSubscription {
            subscribe: subscribe.to_string(),
            unsubscribe: unsubscribe.to_string(),
            name: update.to_string(),
            merge_strategy: Some(MergeStrategy::MergeStorageChanges),
        }],
    );

    let subway_server = server::build(config).await.unwrap();
    let client = Client::with_endpoints([format!("ws://{}", subway_server.addr)]).unwrap();

    let mut first = client.subscribe(subscribe, params.clone(), unsubscribe).await.unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), first.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        json!({
            "block": "0x01",
            "changes": [["0x01", "first"], ["0x02", null]]
        })
    );

    let mut second = client.subscribe(subscribe, params, unsubscribe).await.unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), second.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        json!({
            "block": "0x02",
            "changes": [["0x01", "second"], ["0x02", null]]
        })
    );

    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), first.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        json!({
            "block": "0x03",
            "changes": [["0x02", "first-update"]]
        })
    );
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), second.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        json!({
            "block": "0x03",
            "changes": [["0x02", "second-update"]]
        })
    );

    subway_server.handle.stop().unwrap();
}

#[tokio::test]
async fn multi_key_storage_subscription_survives_upstream_reconnect() {
    let subscribe = "state_subscribeStorage";
    let update = "state_storage";
    let unsubscribe = "state_unsubscribeStorage";
    let params = vec![json!(["0x01", "0x02"])];

    let mut first_builder = TestServerBuilder::new();
    let mut first_subscriptions = first_builder.register_subscription(subscribe, update, unsubscribe);
    let (first_addr, first_handle) = first_builder.build().await;

    let mut second_builder = TestServerBuilder::new();
    let mut second_subscriptions = second_builder.register_subscription(subscribe, update, unsubscribe);
    let (second_addr, _second_handle) = second_builder.build().await;

    let config = config(
        vec![format!("ws://{first_addr}"), format!("ws://{second_addr}")],
        vec![RpcSubscription {
            subscribe: subscribe.to_string(),
            unsubscribe: unsubscribe.to_string(),
            name: update.to_string(),
            merge_strategy: Some(MergeStrategy::MergeStorageChanges),
        }],
    );

    let subway_server = server::build(config).await.unwrap();
    let client = Client::with_endpoints([format!("ws://{}", subway_server.addr)]).unwrap();
    let mut subscription = client.subscribe(subscribe, params, unsubscribe).await.unwrap();

    let first = first_subscriptions.recv().await.unwrap();
    first
        .send(json!({
            "block": "0x01",
            "changes": [["0x01", "before-disconnect"], ["0x02", null]]
        }))
        .await;
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), subscription.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        json!({
            "block": "0x01",
            "changes": [["0x01", "before-disconnect"], ["0x02", null]]
        })
    );

    first_handle.stop().unwrap();
    first_handle.stopped().await;

    let second = tokio::time::timeout(std::time::Duration::from_secs(5), second_subscriptions.recv())
        .await
        .unwrap()
        .unwrap();
    second
        .send(json!({
            "block": "0x02",
            "changes": [["0x01", "after-reconnect"], ["0x02", null]]
        }))
        .await;
    second
        .send(json!({
            "block": "0x03",
            "changes": [["0x02", "later-update"]]
        }))
        .await;

    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), subscription.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        json!({
            "block": "0x02",
            "changes": [["0x01", "after-reconnect"], ["0x02", null]]
        })
    );
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), subscription.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        json!({
            "block": "0x03",
            "changes": [["0x02", "later-update"]]
        })
    );

    subscription.unsubscribe().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), second.sink.closed())
        .await
        .unwrap();

    subway_server.handle.stop().unwrap();
}

#[tokio::test]
async fn multi_key_storage_subscription_backs_off_and_stops_retrying_after_unsubscribe() {
    let subscribe = "state_subscribeStorage";
    let update = "state_storage";
    let unsubscribe = "state_unsubscribeStorage";
    let params = vec![json!(["0x01", "0x02"])];

    let (attempt_tx, mut attempts) = tokio::sync::mpsc::channel(10);
    let mut module = RpcModule::new(());
    module
        .register_subscription(subscribe, update, unsubscribe, move |_, pending_sink, _, _| {
            let attempt_tx = attempt_tx.clone();
            async move {
                pending_sink.accept().await.unwrap();
                attempt_tx.send(()).await.unwrap();
                SubscriptionCloseResponse::NotifErr("closed".into())
            }
        })
        .unwrap();

    let upstream_server = ServerBuilder::default().build("0.0.0.0:0").await.unwrap();
    let upstream_addr = upstream_server.local_addr().unwrap();
    let upstream_handle = upstream_server.start(module);

    let config = config(
        vec![format!("ws://{upstream_addr}")],
        vec![RpcSubscription {
            subscribe: subscribe.to_string(),
            unsubscribe: unsubscribe.to_string(),
            name: update.to_string(),
            merge_strategy: Some(MergeStrategy::MergeStorageChanges),
        }],
    );

    let subway_server = server::build(config).await.unwrap();
    let client = Client::with_endpoints([format!("ws://{}", subway_server.addr)]).unwrap();
    let subscription = client.subscribe(subscribe, params, unsubscribe).await.unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(5), attempts.recv())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), attempts.recv())
        .await
        .unwrap()
        .unwrap();

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(150), attempts.recv())
            .await
            .is_err(),
        "successful-but-closed subscriptions should retain reconnect backoff"
    );

    tokio::time::timeout(std::time::Duration::from_secs(5), attempts.recv())
        .await
        .unwrap()
        .unwrap();
    subscription.unsubscribe().await.unwrap();

    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), attempts.recv())
            .await
            .is_err(),
        "downstream unsubscribe should stop reconnect attempts"
    );

    subway_server.handle.stop().unwrap();
    upstream_handle.stop().unwrap();
}

#[tokio::test]
async fn multi_key_storage_subscription_cleans_up_an_in_flight_retry_after_unsubscribe() {
    let subscribe = "state_subscribeStorage";
    let update = "state_storage";
    let unsubscribe = "state_unsubscribeStorage";
    let params = vec![json!(["0x01", "0x02"])];

    let attempt = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let retry_gate = std::sync::Arc::new(tokio::sync::Notify::new());
    let retry_gate_for_server = retry_gate.clone();
    let (retry_started_tx, mut retry_started) = tokio::sync::mpsc::channel(1);
    let (retry_sink_tx, mut retry_sinks) = tokio::sync::mpsc::channel(1);
    let mut module = RpcModule::new(());
    module
        .register_subscription(subscribe, update, unsubscribe, move |_, pending_sink, _, _| {
            let attempt = attempt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let retry_gate = retry_gate_for_server.clone();
            let retry_started_tx = retry_started_tx.clone();
            let retry_sink_tx = retry_sink_tx.clone();
            async move {
                if attempt == 0 {
                    pending_sink.accept().await.unwrap();
                    return SubscriptionCloseResponse::NotifErr("closed".into());
                }

                retry_started_tx.send(()).await.unwrap();
                retry_gate.notified().await;
                let sink = pending_sink.accept().await.unwrap();
                retry_sink_tx.send(sink).await.unwrap();
                SubscriptionCloseResponse::None
            }
        })
        .unwrap();

    let upstream_server = ServerBuilder::default().build("0.0.0.0:0").await.unwrap();
    let upstream_addr = upstream_server.local_addr().unwrap();
    let upstream_handle = upstream_server.start(module);

    let config = config(
        vec![format!("ws://{upstream_addr}")],
        vec![RpcSubscription {
            subscribe: subscribe.to_string(),
            unsubscribe: unsubscribe.to_string(),
            name: update.to_string(),
            merge_strategy: Some(MergeStrategy::MergeStorageChanges),
        }],
    );

    let subway_server = server::build(config).await.unwrap();
    let client = Client::with_endpoints([format!("ws://{}", subway_server.addr)]).unwrap();
    let subscription = client.subscribe(subscribe, params, unsubscribe).await.unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(1), retry_started.recv())
        .await
        .unwrap()
        .unwrap();
    subscription.unsubscribe().await.unwrap();
    retry_gate.notify_one();

    let retry_sink = tokio::time::timeout(std::time::Duration::from_secs(1), retry_sinks.recv())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), retry_sink.closed())
        .await
        .unwrap();

    subway_server.handle.stop().unwrap();
    upstream_handle.stop().unwrap();
}
