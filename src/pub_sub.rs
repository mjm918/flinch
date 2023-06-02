use log::trace;
use tokio::sync::mpsc::error::SendTimeoutError;
use tokio::sync::mpsc::Sender;
use crate::headers::{Request, PubSubRes};
use crate::utils::TIMEOUT;

#[derive(Clone, Debug)]
pub struct PubSub<M> {
    sender: Sender<Request<M>>
}

impl<M> PubSub<M> where M: Send + 'static
{
    pub fn new(sender: Sender<Request<M>>) -> Self {
        PubSub {
            sender
        }
    }

    pub async fn reg(&self, sender: Sender<M>) -> Result<(), PubSubRes> {
        let res = self.sender.send_timeout(Request::Register(sender), TIMEOUT).await;
        trace!("subscriber registered");
        match res {
            Ok(_) => Ok(()),
            Err(e) => {
                match e {
                    SendTimeoutError::Timeout(_) => Err(PubSubRes::Timeout),
                    SendTimeoutError::Closed(_) => Err(PubSubRes::Closed),
                }
            }
        }
    }

    pub async fn notify(&self, msg: M) -> Result<(), PubSubRes> {
        trace!("notifying message to subscriber");
        let res = self.sender.send_timeout(Request::Dispatch(msg), TIMEOUT).await;
        match res {
            Ok(_) => Ok(()),
            Err(e) => {
                match e {
                    SendTimeoutError::Timeout(_) => Err(PubSubRes::Timeout),
                    SendTimeoutError::Closed(_) => Err(PubSubRes::Closed),
                }
            }
        }
    }
}