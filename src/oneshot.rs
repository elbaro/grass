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

#[cfg(test)]
mod tests {
	use super::*;
	use futures::StreamExt as StdStreamExt;

	#[test]
	fn test_send_recv() {
		timebomb::timeout_ms(
			|| {
				let (sender, flag) = new::<u32>();
				sender.send(98124).unwrap();
				crate::compat::tokio_run(
					async {
						assert!(await!(flag) == Ok(98124));
					},
				);
			},
			1000,
		);
	}

	#[test]
	fn test_clone_recv_send() {
		timebomb::timeout_ms(
			|| {
				let (sender, flag) = new::<u32>();
				let sender1 = sender.clone();
				let sender2 = sender1.clone();
				let flag1 = flag.clone();
				let flag2 = flag1.clone();

				crate::compat::tokio_run(
					async move {
						crate::compat::tokio_spawn(
							async move {
								assert!(await!(flag1) == Ok(1234));
								assert!(await!(flag2) == Ok(1234));
								assert!(await!(flag) == Ok(1234));
							},
						);
						sender2.send(1234).unwrap();
						assert!(sender1.send(4213).is_err());
					},
				);
			},
			1000,
		);
	}

	#[test]
	fn test_send_twice() {
		timebomb::timeout_ms(
			|| {
				let (sender, flag) = new::<u32>();
				sender.send(198124).unwrap();
				crate::compat::tokio_run(
					async move {
						assert!(await!(flag) == Ok(198124));
						assert!(sender.send(2193).is_err());
					},
				);
			},
			1000,
		);
	}

	#[test]
	fn test_take_until() {
		timebomb::timeout_ms(
			|| {
				let (sender, flag) = new::<()>();

				crate::compat::tokio_run(
					async move {
						let vec = vec![6, 7, 8, 9, 10];
						let mut stream = futures::stream::iter(vec.iter()).take_until(flag.map(|_| ()));
						assert_eq!(await!(stream.next()), Some(&6));
						assert_eq!(await!(stream.next()), Some(&7));
						sender.send(()).unwrap();
						assert_eq!(await!(stream.next()), None);
					},
				);
			},
			1000,
		);
	}
}