use std::io::Cursor;

use bytes::Bytes;
use stompr::client::StompClient;
use stompr::frame::{CommandKind, Frame};
use stompr::transport::tcp::config::TcpTransportConfig;
use stompr::transport::tcp::TcpTransport;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{self, Duration};

#[tokio::test]
async fn test_subscription_receives_message() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let payload = b"Hello from server!";
        let frame = Frame::new(CommandKind::Message)
            .with_header("subscription".to_string(), "test_topic".to_string())
            .with_header("content-length".to_string(), payload.len().to_string())
            .with_payload(Bytes::from_static(payload));
        let data = frame.to_bytes();
        socket.write_all(&data).await.unwrap();
        socket.flush().await.unwrap();
    });

    let config = TcpTransportConfig {
        server_address: addr.to_string(),
    };
    let client = StompClient::<TcpTransport>::connect(config).await.unwrap();

    let mut sub_rx = client.subscribe("test_topic".to_string()).await;

    let received_frame = time::timeout(Duration::from_secs(1), sub_rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(received_frame.command, CommandKind::Message);
    assert_eq!(
        received_frame.payload,
        Bytes::from_static(b"Hello from server!")
    );
}

#[tokio::test]
async fn test_send_sends_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            let n = socket.read(&mut byte).await.unwrap();
            if n == 0 {
                break;
            }
            buffer.push(byte[0]);
            if byte[0] == 0 {
                break;
            }
        }
        let data = Bytes::copy_from_slice(&buffer);
        let mut cursor = Cursor::new(&data);
        Frame::parse(&mut cursor).unwrap()
    });

    let config = TcpTransportConfig {
        server_address: addr.to_string(),
    };
    let client = StompClient::<TcpTransport>::connect(config).await.unwrap();

    let payload = b"Hello send!";
    let test_frame = Frame::new(CommandKind::Send)
        .with_header("content-length".to_string(), payload.len().to_string())
        .with_payload(Bytes::from_static(payload));

    client.send(test_frame.clone()).await.unwrap();

    let received_frame = server_handle.await.unwrap();

    assert_eq!(received_frame.command, test_frame.command);
    assert_eq!(received_frame.payload, test_frame.payload);
}

#[tokio::test]
async fn test_error_frame_causes_shutdown() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr = listener
        .local_addr()
        .unwrap();

    tokio::spawn(async move {
        let (mut socket, _) = listener
            .accept()
            .await
            .unwrap();
        let payload = b"Error occurred";
        let error_frame = Frame::new(CommandKind::Error)
            .with_header("message".to_string(), "Test error".to_string())
            .with_header("content-length".to_string(), payload.len().to_string())
            .with_payload(Bytes::from_static(payload));
        let data = error_frame.to_bytes();
        socket
            .write_all(&data)
            .await
            .unwrap();
        socket.flush().await.unwrap();
    });

    let config = TcpTransportConfig {
        server_address: addr.to_string(),
    };
    let client = StompClient::<TcpTransport>::connect(config)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    let payload = b"Test";
    let test_frame = Frame::new(CommandKind::Send)
        .with_header("content-length".to_string(), payload.len().to_string())
        .with_payload(Bytes::from_static(payload));
    let send_result = client.send(test_frame).await;
    assert!(
        send_result.is_err(),
        "Expected error when sending after shutdown"
    );
}

#[tokio::test]
async fn test_unsubscribe_sends_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            let n = socket.read(&mut byte).await.unwrap();
            if n == 0 {
                break;
            }
            buffer.push(byte[0]);
            if byte[0] == 0 {
                break;
            }
        }
        let data = Bytes::copy_from_slice(&buffer);
        let mut cursor = Cursor::new(&data);
        Frame::parse(&mut cursor).unwrap()
    });

    let config = TcpTransportConfig {
        server_address: addr.to_string(),
    };
    let client = StompClient::<TcpTransport>::connect(config).await.unwrap();

    let subscription_id = "sub-123";
    let _sub_rx = client.subscribe("test_topic".to_string()).await;

    client.unsubscribe(subscription_id).await.unwrap();

    let received_frame = server_handle.await.unwrap();

    assert_eq!(received_frame.command, CommandKind::Unsubscribe);
    assert_eq!(
        received_frame.headers.get("id"),
        Some(&subscription_id.to_string())
    );
}

#[tokio::test]
async fn test_disconnect_sends_frame_and_waits_for_receipt() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = Vec::new();
        
        loop {
            let mut byte = [0u8; 1];
            let n = socket.read(&mut byte).await.unwrap();
            if n == 0 { break; }
            buffer.push(byte[0]);
            if byte[0] == 0 { break; }
        }

        let data = Bytes::copy_from_slice(&buffer);
        let mut cursor = Cursor::new(&data);
        let frame = Frame::parse(&mut cursor).unwrap();
        assert_eq!(frame.command, CommandKind::Disconnect);

        let receipt_id = frame.headers.get("receipt").expect("DISCONNECT frame should have a receipt");

        let receipt_frame = Frame::new(CommandKind::Receipt)
            .with_header("receipt-id".to_string(), receipt_id.clone());
        socket.write_all(&receipt_frame.to_bytes()).await.unwrap();
        socket.flush().await.unwrap();
    });

    let config = TcpTransportConfig {
        server_address: addr.to_string(),
    };
    let client = StompClient::<TcpTransport>::connect(config).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    client.disconnect().await.unwrap();

    server_handle.await.unwrap();
}
