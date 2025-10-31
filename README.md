# wplace scraper / pumpkin (event) matcher

vibe coded wplace scraper with proxy rotation, and template matcher.

dont expect any support on this cuz this codebase is fucking ass and i dont understand rust well enough to know how to
fix most if any issues other than asking cursor "please fix".

## Usage

1. copy `.env.example` to `.env`, and configure settings
2. create `poxies.json` and add an array of proxy connection strings (ie:
   `["http://user:pass@host:port", "http://user:pass@host:port"]`)
3. run `cargo run --release`

## Requirements

Rust (obviously) - <https://rust-lang.org/> You need gcc+mingw, if on windows, you should get
[WinLibs GCC+MinGW-W64](https://winlibs.com/)

## Proxies

(Not sponsored) get some free proxies from <https://webshare.io>, or pay for their shared datacenter proxies (their ToS
doesn't allow residential proxies for gaming purposes, and wplace is a game).
