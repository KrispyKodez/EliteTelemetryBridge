# Third-party notices

The portable Windows build bundles Python and the following runtime
dependencies. Their license texts are copied into the `licenses` directory by
`build-release.ps1`.

| Component | Version | License |
| --- | ---: | --- |
| Python | 3.13 | Python Software Foundation License 2.0 |
| aiohttp | 3.14.1 | Apache-2.0 AND MIT |
| aiohappyeyeballs | 2.7.1 | PSF-2.0 |
| aiosignal | 1.4.0 | Apache-2.0 |
| attrs | 26.1.0 | MIT |
| frozenlist | 1.8.0 | Apache-2.0 |
| multidict | 6.7.1 | Apache-2.0 |
| propcache | 0.5.2 | Apache-2.0 |
| yarl | 1.24.2 | Apache-2.0 |
| idna | 3.18 | BSD-3-Clause |
| watchdog | 6.0.0 | Apache-2.0 |

PyInstaller is used to create the release but is not included as an importable
runtime package. Its bootloader is distributed under the PyInstaller license,
which includes an exception permitting bundled applications.
