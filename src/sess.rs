use tokio::sync::mpsc::error::SendTimeoutError;
use tokio::sync::mpsc::Sender;
use crate::hdrs::{Request, SessionRes, TIMEOUT};

#[derive(Clone, Debug)]
pub struct Session<M> {
    sender: Sender<Request<M>>
}

impl<M> Session<M> where M: Send + 'static
{
    pub(crate) fn new(sender: Sender<Request<M>>) -> Self {
        Session {
            sender
        }
    }

    pub async fn reg(&self, sender: Sender<M>) -> Result<(), SessionRes> {
        let res = self.sender.send_timeout(Request::Register(sender), TIMEOUT).await;
        match res {
            Ok(_) => Ok(()),
            Err(e) => {
                match e {
                    SendTimeoutError::Timeout(_) => Err(SessionRes::Timeout),
                    SendTimeoutError::Closed(_) => Err(SessionRes::Closed),
                }
            }
        }
    }

    pub async fn notify(&self, msg: M) -> Result<(), SessionRes> {
        let res = self.sender.send_timeout(Request::Dispatch(msg), TIMEOUT).await;
        match res {
            Ok(_) => Ok(()),
            Err(e) => {
                match e {
                    SendTimeoutError::Timeout(_) => Err(SessionRes::Timeout),
                    SendTimeoutError::Closed(_) => Err(SessionRes::Closed),
                }
            }
        }
    }

}