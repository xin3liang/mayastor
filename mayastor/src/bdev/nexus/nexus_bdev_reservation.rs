//! Implements NVMe reservation operations on a nexus.

use crate::{
    bdev::nexus::nexus_bdev::{Error, Nexus},
    core::BdevHandle,
};

impl Nexus {
    /// Reservation Register on all children
    pub async fn resv_register(
        &self,
        current_key: u64,
        new_key: u64,
        register_action: u8,
        cptpl: u8,
    ) -> Result<(), Error> {
        if let Ok(h) = BdevHandle::open_with_bdev(&self.bdev, true) {
            match h
                .nvme_resv_register(
                    current_key,
                    new_key,
                    register_action,
                    cptpl,
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(e) => Err(Error::FailedResvRegister {
                    name: self.bdev.name(),
                    source: e,
                }),
            }
        } else {
            Err(Error::FailedGetHandle)
        }
    }

    /// Reservation Acquire on all children
    pub async fn resv_acquire(
        &self,
        current_key: u64,
        preempt_key: u64,
        acquire_action: u8,
        resv_type: u8,
    ) -> Result<(), Error> {
        if let Ok(h) = BdevHandle::open_with_bdev(&self.bdev, true) {
            match h
                .nvme_resv_acquire(
                    current_key,
                    preempt_key,
                    acquire_action,
                    resv_type,
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(e) => Err(Error::FailedResvAcquire {
                    name: self.bdev.name(),
                    source: e,
                }),
            }
        } else {
            Err(Error::FailedGetHandle)
        }
    }
}
