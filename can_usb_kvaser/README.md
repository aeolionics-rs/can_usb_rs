CAN USB Kvaser
==============

A pure Rust driver for Kvaser USB CAN interfaces, such as the Leaf.

# Platforms

Kvaser provide native C drivers for Linux and Windows that can be utilized
thorough the [can_hal](https://github.com/can-hal-rs-contributors/can-hal-rs/tree/master/can-hal-kvaser) interface.
This requires the Kvaser driver to be installed.


## Linux
Kvaser provide a kernel driver that is installed by default in many distributions
and provides access to the interface through the socketcan APIs.
This driver can be used as an alternative if the kernel driver
is not available.

## MacOS
Kvaser do not provide a user-space driver for their devices on MacOS. 
The [MacCAN](https://www.mac-can.com/) project does provide an [implementation](https://github.com/mac-can/KvaserCAN-Library) with C
and C++ interfaces that can be used through an FFI binding.
This crate provides a pure Rust implementation verified on MacOS.

## Windows
This driver may be used as alternative to the Kvaser Windows SDK. 

# License & Contributions
All drivers are licensed under the Apache License Version 2.0.