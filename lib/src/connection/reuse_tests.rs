#![allow(deprecated)]

use super::*;
use tokio::net::TcpListener;

async fn connection() -> (Connection, TcpStream) {
    connection_with_timeout(Duration::from_millis(20)).await
}

async fn connection_with_timeout(recv_timeout: Duration) -> (Connection, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stream = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    (
        Connection::create(stream, Version::V4_4, recv_timeout),
        peer,
    )
}

#[tokio::test]
async fn record_does_not_complete_request_but_all_terminal_summaries_do() {
    let (mut connection, _peer) = connection().await;
    for signature in [0x70, 0x7f, 0x7e] {
        assert!(connection.is_reusable());
        connection.send(BoltRequest::reset()).await.unwrap();
        assert!(!connection.is_reusable());
        connection.complete_response(&[0xb1, 0x71, 0x90]).unwrap();
        assert!(!connection.is_reusable());
        connection
            .complete_response(&[0xb1, signature, 0xa0])
            .unwrap();
        assert!(connection.is_reusable());
    }
}

#[tokio::test]
async fn decode_failure_leaves_connection_unusable() {
    let (mut connection, mut peer) = connection().await;
    connection.send(BoltRequest::reset()).await.unwrap();
    peer.write_u16(3).await.unwrap();
    peer.write_all(&[0xb1, 0xff, 0xa0]).await.unwrap();
    peer.write_u16(0).await.unwrap();
    assert!(connection.recv().await.is_err());
    assert!(!connection.is_reusable());
    assert!(matches!(
        connection.send(BoltRequest::reset()).await,
        Err(Error::ConnectionError)
    ));
}

#[tokio::test]
async fn unsolicited_terminal_summary_is_not_reusable() {
    let (mut connection, mut peer) = connection().await;
    peer.write_u16(3).await.unwrap();
    peer.write_all(&[0xb1, 0x70, 0xa0]).await.unwrap();
    peer.write_u16(0).await.unwrap();
    assert!(connection.recv().await.is_err());
    assert!(!connection.is_reusable());
}

#[tokio::test]
async fn externally_cancelled_receive_leaves_connection_unusable() {
    let (mut connection, _peer) = connection_with_timeout(Duration::from_secs(60)).await;
    connection.send(BoltRequest::reset()).await.unwrap();
    assert!(!connection.is_reusable());
    {
        let receive = connection.recv();
        tokio::pin!(receive);
        tokio::select! {
            biased;
            _ = &mut receive => unreachable!("the peer never answers"),
            _ = tokio::task::yield_now() => {}
        }
    }
    assert!(!connection.is_reusable());
    assert!(matches!(
        connection.send(BoltRequest::reset()).await,
        Err(Error::ConnectionError)
    ));
}

#[tokio::test]
async fn receive_timeout_leaves_connection_unusable() {
    let (mut connection, _peer) = connection().await;
    assert!(matches!(
        connection.reset().await,
        Err(Error::ConnectionTimedOut)
    ));
    assert!(!connection.is_reusable());
    assert!(matches!(
        connection.send(BoltRequest::reset()).await,
        Err(Error::ConnectionError)
    ));
}

#[cfg(feature = "unstable-bolt-protocol-impl-v2")]
#[tokio::test]
async fn typed_reset_receive_timeout_leaves_connection_unusable() {
    let (mut connection, _peer) = connection().await;
    assert!(matches!(
        connection.reset().await,
        Err(Error::ConnectionTimedOut)
    ));
    assert!(!connection.is_reusable());
    assert!(matches!(
        connection.reset().await,
        Err(Error::ConnectionError)
    ));
}
