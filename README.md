CAN USB Rust
============

Drivers for CAN interfaces that connect using USB, written as pure Rust without
dependencies on external driver libraries.

# CAN USB Kvaser

[can_usb_kvaser](can_usb_kvaser): USB interfaces by Kvaser AB, such as the Leaf range.

# CAN USB gs_usb

[can_usb_gs](can_usb_gs): interfaces that speak the gs_usb protocol, such as
candleLight and its descendants, without a kernel driver (macOS, Windows).

[gs_usb_protocol](gs_usb_protocol): the gs_usb wire format, `no_std` with no
dependencies, for hosts and for device firmware.

# License & Contributions
All drivers are licensed under the Apache License Version 2.0.
`gs_usb_protocol` is licensed under either the Apache License Version 2.0 or
the MIT license, at your option.
