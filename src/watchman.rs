use anyhow::Result;
use tokio::sync::mpsc::{channel, Sender};

use crate::errors::WatcherError;
use crate::headers::{DestinationDown, Request, WatcherState};
use crate::pub_sub::PubSub;

pub struct Watchman<M> {
    idx: usize,
    chans: Vec<Sender<M>>,
}

impl<M> Watchman<M> where M: Clone + Send + 'static {
    pub fn new(chans: Vec<Sender<M>>) -> Result<Self, WatcherError> {
        if let Err(err) = Self::multi_chk(&chans) {
            return Err(err);
        }
        Ok(Self {
            idx: 0,
            chans,
        })
    }

    pub fn start(mut self) -> PubSub<M> {
        let (sx, mut rx) = channel(30);
        let session = PubSub::new(sx);
        tokio::spawn(async move {
            loop {
                let res = rx.recv().await;
                if let WatcherState::Disconnected = self.recv(res).await {
                    return ();
                }
            }
        });
        session
    }

    async fn recv(&mut self, res: Option<Request<M>>) -> WatcherState {
        match res {
            Some(req) => {
                match req {
                    Request::Register(sender) => {
                        match self.chk(&sender) {
                            Ok(_) => {
                                self.chans.push(sender);
                                WatcherState::Continue
                            }
                            Err(_) => {
                                return WatcherState::Continue;
                            }
                        }
                    }
                    Request::Dispatch(msg) => {
                        let _ = self.notify(msg).await;
                        WatcherState::Continue
                    }
                }
            }
            None => WatcherState::Disconnected
        }
    }

    async fn notify(&mut self, msg: M) -> Result<(), DestinationDown<M>> {
        if self.chans.len() == 0 {
            return Ok(());
        }
        self.broadcast(msg).await;
        Ok(())
    }

    async fn broadcast(&mut self, msg: M) {
        if self.chans.len() == 0 {
            return;
        }
        if self.chans.len() > 1 {
            for index in 0..=(self.chans.len() - 2) {
                let msg = msg.clone();
                let _ = self.chans[index].send(msg).await;
            }
        }
        let _ = self.chans[self.chans.len() - 1].send(msg).await;
    }

    #[allow(dead_code)]
    fn next(&mut self) -> usize {
        let mut index = self.idx;
        self.idx += 1;
        if index >= self.chans.len() {
            self.idx = 0;
            index = 0;
        }
        return index;
    }

    fn multi_chk(chans: &Vec<Sender<M>>) -> Result<(), WatcherError> {
        for (idx, outer_dst) in chans.iter().enumerate() {
            for (jdx, inner_dst) in chans.iter().enumerate() {
                if idx != jdx && outer_dst.same_channel(inner_dst) {
                    return Err(WatcherError::SendersRepetitive);
                }
            }
        }
        Ok(())
    }

    fn chk(&self, chan: &Sender<M>) -> Result<(), WatcherError> {
        for (_, dst) in self.chans.iter().enumerate() {
            if chan.same_channel(dst) {
                return Err(WatcherError::SendersRepetitive);
            }
        }
        Ok(())
    }
}