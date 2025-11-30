#!/usr/bin/env sh

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
sudo yum -y groupinstall "Development Tools"
sudo yum -y install openssl-devel
git clone https://github.com/marcbowes/riv25-codetalk.git
cd riv25-codetalk/
cd helper/
cargo build --release
