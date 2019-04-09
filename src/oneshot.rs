use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
// use futures::lock::Mutex;
use futures::{task::Waker, Future, FutureExt, Poll, Stream};

#[derive(Clone)]
pub struct OneshotSender<T = ()> {
	inner: Arc<Mutex<Option<futures::channel::oneshot::Sender<T>>>>,
}

impl<T> OneshotSender<T> {
	pub fn send(&self, value: T) -> Result<(), &'static str> {
		let inner = self.inner.clone();
		let mut guard = inner.lock().unwrap();
		if let Some(sender) = guard.take() {
			sender.send(value).map_err(|_| "Oneshot Sender send() fail")
		} else {
			Err("OneshotSender is already used")
		}
	}
}

pub type OneshotFlag<T = ()> = futures::future::Shared<futures::channel::oneshot::Receiver<T>>;

pub fn new<T: Clone>() -> (OneshotSender<T>, OneshotFlag<T>) {
	let (s, r) = futures::channel::oneshot::channel();
	let s = OneshotSender {
		inner: Arc::new(Mutex::new(Some(s))),
	};
	let r = r.shared();
	(s, r)
}

#[derive(Clone, Debug)]
pub struct TakeUntil<S: Stream, F: Future> {
	stream: S,
	until: F,
}

impl<S: Stream, F: Future<Output = ()>> TakeUntil<S, F> {
	pin_utils::unsafe_pinned!(stream: S);
	pin_utils::unsafe_pinned!(until: F);
}

pub trait StreamExt: Stream {
	fn take_until<F: Future<Output = ()>>(self, until: F) -> TakeUntil<Self, F>
	where
		Self: Sized,
	{
		TakeUntil {
			stream: self,
			until,
		}
	}
}
impl<T: Stream> StreamExt for T {}

impl<S: Stream, F: Future<Output = ()>> Stream for TakeUntil<S, F> {
	type Item = S::Item;

	fn poll_next(mut self: Pin<&mut Self>, waker: &Waker) -> Poll<Option<Self::Item>> {
		match self.as_mut().until().poll(waker) {
			Poll::Ready(_) => {
				// future resolved -- terminate stream
				return Poll::Ready(None);
			}
			Poll::Pending => {}
		};
		self.as_mut().stream().poll_next(waker)
	}
}

// #[cfg(test)]
// mod tests {
//     use super::{OneshotFlag, OneshotSender, new, StreamExt};
//     use futures::{channel::mpsc, prelude::*, Poll};

//     #[test]
//     fn test_() {

// 	}
// }
