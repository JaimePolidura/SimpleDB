use std::sync::atomic::{AtomicI8, AtomicPtr, AtomicUsize};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

const ACTIVE: i8 = 0;
const TO_CHANGE: i8 = 0;

pub struct AtomicSharedRef<T> {
    shared: AtomicPtr<SharedRef<T>>,
    state: AtomicI8,
}

pub struct SharedRef<T> {
    users: AtomicUsize,
    pub shared_ref: T,
}

impl<T> AtomicSharedRef<T> {
    pub fn create(ptr: T) -> AtomicSharedRef<T> {
        AtomicSharedRef {
            state: AtomicI8::new(ACTIVE),
            shared: AtomicPtr::new(Box::into_raw(Box::new(SharedRef::create(ptr))))
        }
    }

    pub fn load_ref(&self) -> &SharedRef<T> {
        loop {
            let value = self.shared.load(Acquire);
            unsafe {(*value).users.fetch_add(1, Relaxed);}

            if self.state.load(Acquire) == ACTIVE {
                return unsafe { &(*value) };
            } else {
                unsafe {(*value).users.fetch_sub(1, Relaxed);}
            }
        }
    }

    pub fn unload_ref(&self, reference: &SharedRef<T>) {
        reference.users.fetch_sub(1, Relaxed);
    }

    pub fn try_cas(&self, new_ptr: T) -> Result<T, ()> {
        if self.state.compare_exchange(ACTIVE, TO_CHANGE, Acquire, Relaxed).is_err() {
            return Err(());
        }

        let old_ptr: * mut SharedRef<T> = self.shared.load(Acquire);

        self.shared.store(
            Box::into_raw(Box::new(SharedRef::create(new_ptr))),
            Release
        );

        while unsafe { (*old_ptr).users.load(Relaxed) > 0 } {
            std::thread::yield_now();
        }

        self.state.store(ACTIVE, Release);

        Ok((unsafe {Box::from_raw(old_ptr)}).shared_ref)
    }
}

impl<T> SharedRef<T> {
    pub fn create(value: T) -> SharedRef<T> {
        SharedRef {
            users: AtomicUsize::new(0),
            shared_ref: value
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::time::Duration;
    use crate::atomic_shared_ref::AtomicSharedRef;

    #[test]
    fn load_unload() {
        let mut vector_ref: Arc<AtomicSharedRef<Vec<u8>>> = Arc::new(AtomicSharedRef::create(Vec::new()));

        let t1 =  {
            let vector_ref = vector_ref.clone();

            std::thread::spawn(move || {
                for i in 0..10000 {
                    let vector_ref_1 = vector_ref.load_ref();
                    std::thread::sleep(Duration::from_secs(20));
                    vector_ref.unload_ref(vector_ref_1);
                }
            });
        };

        let vector_cas_result = vector_ref.try_cas(Vec::new());
        assert!(vector_cas_result.is_ok());
    }
}