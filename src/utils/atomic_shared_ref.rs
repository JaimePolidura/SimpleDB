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
    pub fn new(ptr: T) -> AtomicSharedRef<T> {
        return AtomicSharedRef {
            state: AtomicI8::new(ACTIVE),
            shared: AtomicPtr::new(Box::into_raw(Box::new(SharedRef::new(ptr))))
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

    pub fn try_cas(&mut self, new_ptr: T) -> Result<T, ()> {
        if self.state.compare_exchange(ACTIVE, TO_CHANGE, Acquire, Relaxed).is_err() {
            return Err(());
        }

        let old_ptr: * mut SharedRef<T> = self.shared.load(Acquire);

        self.shared.store(
            Box::into_raw(Box::new(SharedRef::new(new_ptr))),
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
    pub fn new(value: T) -> SharedRef<T> {
        return SharedRef {
            users: AtomicUsize::new(0),
            shared_ref: value
        }
    }
}