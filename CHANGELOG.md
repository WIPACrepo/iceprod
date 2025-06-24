# Changelog

<!--next-version-placeholder-->

## v3.0.55 (2025-06-24)

### Other

* Fix weird heredoc login shell issues by defining the shell ([#431](https://github.com/WIPACrepo/iceprod/issues/431)) ([`152799c`](https://github.com/WIPACrepo/iceprod/commit/152799c636af5472ee517e2437a734a204528b2c))

## v3.0.54 (2025-06-24)

### Other

* Fix condor/iceprod check race condition ([#430](https://github.com/WIPACrepo/iceprod/issues/430)) ([`3ac6642`](https://github.com/WIPACrepo/iceprod/commit/3ac6642cb7120e89829a7f1b9dca660d1e7704c3))

## v3.0.53 (2025-06-10)

### Other

* Add a longer timeout for gridftp transfers ([#429](https://github.com/WIPACrepo/iceprod/issues/429)) ([`0bfbdad`](https://github.com/WIPACrepo/iceprod/commit/0bfbdad9c86a7e9e882b5873ee1bd9e88964ea08))

## v3.0.52 (2025-04-23)

### Other

* Fix submit bug when executable is a url ([#428](https://github.com/WIPACrepo/iceprod/issues/428)) ([`766d684`](https://github.com/WIPACrepo/iceprod/commit/766d684361d0a2f449da39c3273022c1fe9d0bb8))

## v3.0.51 (2025-04-11)

### Other

* Update default resources ([#427](https://github.com/WIPACrepo/iceprod/issues/427)) ([`9c72778`](https://github.com/WIPACrepo/iceprod/commit/9c72778272f8d460a9065c124361aca9b91df964))

## v3.0.50 (2025-03-31)

### Other

* Website config link and docs ([#426](https://github.com/WIPACrepo/iceprod/issues/426)) ([`2873012`](https://github.com/WIPACrepo/iceprod/commit/2873012dbc204ebd2f1920e1ff34f94d4f9852e0))

## v3.0.49 (2025-02-07)

### Other

* Pin boto3 to before aws broke it for third parties ([#422](https://github.com/WIPACrepo/iceprod/issues/422)) ([`c7c35f2`](https://github.com/WIPACrepo/iceprod/commit/c7c35f2f0d7db39e2e42a88a9291c375f5c43334))

## v3.0.48 (2025-02-07)

### Other

* Reduce cardinality in api metrics to keep from blowing up prometheus ([#421](https://github.com/WIPACrepo/iceprod/issues/421)) ([`867d564`](https://github.com/WIPACrepo/iceprod/commit/867d564f4f684dcfd8910fc65fcb2501286cf0f9))

## v3.0.47 (2025-02-05)

### Other

* Fix prometheus port on dataset monitor ([#420](https://github.com/WIPACrepo/iceprod/issues/420)) ([`a772ddb`](https://github.com/WIPACrepo/iceprod/commit/a772ddb843563ea74b8be7c8590c4399be3c7b89))

## v3.0.46 (2025-01-29)

### Other

* Add asyncio task monitor for api services ([#419](https://github.com/WIPACrepo/iceprod/issues/419)) ([`e398c1e`](https://github.com/WIPACrepo/iceprod/commit/e398c1ebdf8a6be1e5d601121cca58e9dfbce316))

## v3.0.45 (2025-01-28)

### Other

* <bot> update requirements.txt ([`0ac4eaa`](https://github.com/WIPACrepo/iceprod/commit/0ac4eaa529c1f2e500f2a9894c27f0376a2c1640))
* <bot> update requirements-tests.txt ([`3d40320`](https://github.com/WIPACrepo/iceprod/commit/3d40320c08ca396473a98d4a47aab4417da54224))
* <bot> update requirements-docs.txt ([`b907c09`](https://github.com/WIPACrepo/iceprod/commit/b907c097f722fa4692a3e6e927a93a32a678109d))
* Add prometheus metrics ([#418](https://github.com/WIPACrepo/iceprod/issues/418)) ([`6e51584`](https://github.com/WIPACrepo/iceprod/commit/6e51584c6c8638d92a8eb385b6cf43bac35cc8e1))

## v3.0.44 (2025-01-21)

### Other

* Fix materialize bug causing extra jobs ([#417](https://github.com/WIPACrepo/iceprod/issues/417)) ([`26b5312`](https://github.com/WIPACrepo/iceprod/commit/26b5312c42beaa4634da19aa50e52350ba13ec72))

## v3.0.43 (2025-01-10)

### Other

* Actually filter by gpu tasks ([#415](https://github.com/WIPACrepo/iceprod/issues/415)) ([`70b49e6`](https://github.com/WIPACrepo/iceprod/commit/70b49e6d4dab1ae732cf64befa4ec8bca7cd16c0))

## v3.0.42 (2025-01-10)

### Other

* Task idle -> waiting gpu tasks separately ([#414](https://github.com/WIPACrepo/iceprod/issues/414)) ([`d2b3f51`](https://github.com/WIPACrepo/iceprod/commit/d2b3f51aae02b1699c24052077d5083f4ea3f4fb))

## v3.0.41 (2025-01-03)

### Other

* Update task queued/processing when doing cross-check ([#413](https://github.com/WIPACrepo/iceprod/issues/413)) ([`5160c7c`](https://github.com/WIPACrepo/iceprod/commit/5160c7cc6ec69694fea79eab9ac0ed145e6810af))

## v3.0.40 (2025-01-03)

### Other

* Attempt to fix iceprod - condor queue sync issues ([#412](https://github.com/WIPACrepo/iceprod/issues/412)) ([`bc84d94`](https://github.com/WIPACrepo/iceprod/commit/bc84d9488e599154611f1219e6ac11586fced552))

## v3.0.39 (2024-12-24)

### Other

* Fix prio bug ([#411](https://github.com/WIPACrepo/iceprod/issues/411)) ([`3d4e416`](https://github.com/WIPACrepo/iceprod/commit/3d4e416dc1f01e6381f23a88551b0032d5ef94e2))

## v3.0.38 (2024-12-24)

### Other

* Make sure the job actually completed, and didn't get held or removed ([#409](https://github.com/WIPACrepo/iceprod/issues/409)) ([`021f3dd`](https://github.com/WIPACrepo/iceprod/commit/021f3dd8d89febc23ca3fbe21c96bd37e031e675))

## v3.0.37 (2024-12-02)

### Other

* Minor priorty and x509 fixes ([#408](https://github.com/WIPACrepo/iceprod/issues/408)) ([`d72f182`](https://github.com/WIPACrepo/iceprod/commit/d72f1827c03ed5d9135a7d61ac66dfb52768f3dd))

## v3.0.36 (2024-11-22)

### Other

* Add a dataset age factor to priority ([#406](https://github.com/WIPACrepo/iceprod/issues/406)) ([`2ca9271`](https://github.com/WIPACrepo/iceprod/commit/2ca92717f2ac34c48b1c9c3525ccbe27b4dbbed3))

## v3.0.35 (2024-11-20)

### Other

* Do not use the JEL for completions, as the statistics are incomplete ([#405](https://github.com/WIPACrepo/iceprod/issues/405)) ([`0122974`](https://github.com/WIPACrepo/iceprod/commit/0122974a6f46ad6ad1cb2c731a7611e1445f3b43))

## v3.0.34 (2024-11-14)

### Other

* Add a flag to not auto-complete certain datasets ([#403](https://github.com/WIPACrepo/iceprod/issues/403)) ([`5d08da6`](https://github.com/WIPACrepo/iceprod/commit/5d08da68e2e408d4d9d4c268c9f859c70bd77c9d))

## v3.0.33 (2024-11-12)

### Other

* Several submission fixes ([#402](https://github.com/WIPACrepo/iceprod/issues/402)) ([`eff160d`](https://github.com/WIPACrepo/iceprod/commit/eff160d5f6ebbcd5eb879ff2cb769b46ea031a65))

## v3.0.32 (2024-10-31)

### Other

* Fix materialization case where job gets created and service is killed ([#401](https://github.com/WIPACrepo/iceprod/issues/401)) ([`cfd92e2`](https://github.com/WIPACrepo/iceprod/commit/cfd92e2b724116c4b1f73cb55622429794b0483e))

## v3.0.31 (2024-10-27)

### Other

* Fix materialization task count when buffering an existing job ([#400](https://github.com/WIPACrepo/iceprod/issues/400)) ([`8301751`](https://github.com/WIPACrepo/iceprod/commit/83017516c37c2ee6dbcb4acb33b9cb12409d5890))

## v3.0.30 (2024-10-27)

### Other

* Add more error reasons to reset instead of fail ([#399](https://github.com/WIPACrepo/iceprod/issues/399)) ([`c166b2e`](https://github.com/WIPACrepo/iceprod/commit/c166b2edcf5f4b4b9b410c113452c0bde6199524))

## v3.0.29 (2024-10-26)

### Other

* Delete jel and day dir if empty ([#398](https://github.com/WIPACrepo/iceprod/issues/398)) ([`667d7a2`](https://github.com/WIPACrepo/iceprod/commit/667d7a2aed86e79eb2a61b457d684990bfec7edf))

## v3.0.28 (2024-10-25)

### Other

* Clean up job submit dirs more rapidly after completion ([#397](https://github.com/WIPACrepo/iceprod/issues/397)) ([`9546be8`](https://github.com/WIPACrepo/iceprod/commit/9546be8be07b91065530dca2b76b347a35683d28))

## v3.0.27 (2024-10-21)

### Other

* Reset iceprod tasks if not present on the queue ([#396](https://github.com/WIPACrepo/iceprod/issues/396)) ([`c3c95c8`](https://github.com/WIPACrepo/iceprod/commit/c3c95c85652d899d5c11a831413984cf5852b38e))

## v3.0.26 (2024-10-19)

### Other

* Do cross-check with condor_q and condor_history ([#395](https://github.com/WIPACrepo/iceprod/issues/395)) ([`9ff909f`](https://github.com/WIPACrepo/iceprod/commit/9ff909f41069d2308be428eb2424ecf329ff419d))

## v3.0.25 (2024-10-17)

### Other

* Ignore condor log error ([#394](https://github.com/WIPACrepo/iceprod/issues/394)) ([`57c96cd`](https://github.com/WIPACrepo/iceprod/commit/57c96cd1878dd4a5e8e21e932a0dd29678fb6b4e))

## v3.0.24 (2024-10-17)

### Other

* Fix input/output options in basic_submit arguments ([#393](https://github.com/WIPACrepo/iceprod/issues/393)) ([`79813b0`](https://github.com/WIPACrepo/iceprod/commit/79813b03fcaa9e5381f080e8ebf6e91f32013fac))

## v3.0.23 (2024-10-16)

### Other

* Ignore missing config for dataset details ([#392](https://github.com/WIPACrepo/iceprod/issues/392)) ([`0eecd15`](https://github.com/WIPACrepo/iceprod/commit/0eecd1538635e0aa541b2623530bea75b34feaf6))

## v3.0.22 (2024-10-16)

### Other

* Set rest DB timeout and write concern ([#391](https://github.com/WIPACrepo/iceprod/issues/391)) ([`d4311cd`](https://github.com/WIPACrepo/iceprod/commit/d4311cdee161b7043b59774ca49921e43b6132d2))

## v3.0.21 (2024-10-16)

### Other

* Fix basic_submit url to api ([#390](https://github.com/WIPACrepo/iceprod/issues/390)) ([`8351af9`](https://github.com/WIPACrepo/iceprod/commit/8351af983f2b73785ffc1e7d42093aaee5535086))

## v3.0.20 (2024-10-15)

### Other

* Add validation for config json ([#389](https://github.com/WIPACrepo/iceprod/issues/389)) ([`6562371`](https://github.com/WIPACrepo/iceprod/commit/6562371171a5b0db6db6c54d7d931f6a0ea8204f))

## v3.0.19 (2024-10-15)

### Other

* Fix sending configs with the condor job ([#388](https://github.com/WIPACrepo/iceprod/issues/388)) ([`bd8e248`](https://github.com/WIPACrepo/iceprod/commit/bd8e24848457f3c8ee85017d8a493f81f889c03e))

## v3.0.18 (2024-10-15)

### Other

* Fix basic_submit batchsys ([`c7b6b74`](https://github.com/WIPACrepo/iceprod/commit/c7b6b74413886ce922c3cfd5bdb37cbbc12e5bdb))
* Basic submit config fix ([#387](https://github.com/WIPACrepo/iceprod/issues/387)) ([`34dc1d7`](https://github.com/WIPACrepo/iceprod/commit/34dc1d7731c248289c84c72de7ef9a1b81340f00))

## v3.0.17 (2024-10-11)

### Other

* Fix update_task_priority to operate on idle tasks ([#386](https://github.com/WIPACrepo/iceprod/issues/386)) ([`c7bbe0e`](https://github.com/WIPACrepo/iceprod/commit/c7bbe0ef7e7db6ad3e3e1db6acf79bd74f5d7fd0))

## v3.0.16 (2024-10-10)

### Other

* Make flake8 happy ([`c2b4316`](https://github.com/WIPACrepo/iceprod/commit/c2b4316e0b968e8b2dac0ad59de0e327e973a439))
* Convert the logger to a daily rollover, with 1 week kept ([`d11d863`](https://github.com/WIPACrepo/iceprod/commit/d11d863bffea18ebdc768931c9b367490f9b3a3d))
* Better handling for transient errors ([`87b8855`](https://github.com/WIPACrepo/iceprod/commit/87b88556f1b4350d187d5af8dec90b0ebab55c24))

## v3.0.15 (2024-10-10)

### Other

* <bot> update requirements.txt ([`2d93bd9`](https://github.com/WIPACrepo/iceprod/commit/2d93bd9e66e465e86ac6f1b50a53b896c9de3daf))
* <bot> update requirements-tests.txt ([`25daca7`](https://github.com/WIPACrepo/iceprod/commit/25daca706c6fb70f8971cddd1feb11e4bbf117a7))
* <bot> update requirements-docs.txt ([`65cb7c2`](https://github.com/WIPACrepo/iceprod/commit/65cb7c29c5aeeed948eb16b223816fa776876e18))
* Only search for idle and waiting task counts when doing queue_tasks ([`3a60c7a`](https://github.com/WIPACrepo/iceprod/commit/3a60c7a86943e6ac1d2bd432cab95184b44bbc4b))
* Separate idle and waiting tasks for dataset view ([`27fb372`](https://github.com/WIPACrepo/iceprod/commit/27fb3729693c918b57c1a8d8141c3e88cf55a52f))
* Fix materialization end-of-queue ([`d60610d`](https://github.com/WIPACrepo/iceprod/commit/d60610d4d866306272539101f3768ce8c7e155a4))

## v3.0.14 (2024-10-09)

### Other

* <bot> update requirements.txt ([`163330e`](https://github.com/WIPACrepo/iceprod/commit/163330edd47167b78bb405e0fda531fdccbbfb1e))
* <bot> update requirements-tests.txt ([`2f3e505`](https://github.com/WIPACrepo/iceprod/commit/2f3e505e855d8c38f252f90192050bbac7523928))
* <bot> update requirements-docs.txt ([`9b254cc`](https://github.com/WIPACrepo/iceprod/commit/9b254cc0e511e5c04d39447aff5d82c11d8174c1))
* Move back to running gridftp transfers inside the condor job ([#382](https://github.com/WIPACrepo/iceprod/issues/382)) ([`64957b1`](https://github.com/WIPACrepo/iceprod/commit/64957b18920ac5f353210d0b2baf1bd6a06c5fd7))

## v3.0.13 (2024-10-07)

### Other

* <bot> update requirements.txt ([`be82643`](https://github.com/WIPACrepo/iceprod/commit/be826430e365072a8f1d7840cf4b01cfaf4f724d))
* <bot> update requirements-tests.txt ([`9bce64e`](https://github.com/WIPACrepo/iceprod/commit/9bce64eadb58e23a5c95448a5db6dcd2ac1af574))
* <bot> update requirements-docs.txt ([`7f7fa63`](https://github.com/WIPACrepo/iceprod/commit/7f7fa631c86a0eed3dccc6cd34eccd3a3f6c8e44))

## v3.0.12 (2024-10-07)

### Other

* Fix exists transfers and classad import errors ([#381](https://github.com/WIPACrepo/iceprod/issues/381)) ([`646dba1`](https://github.com/WIPACrepo/iceprod/commit/646dba1ed0d57521fde8678589284acc107ad5ff))

## v3.0.11 (2024-10-04)

### Other

* Only use a single os for container selection ([`00238e4`](https://github.com/WIPACrepo/iceprod/commit/00238e48ac592125291441dabf2138d945e0d3a0))

## v3.0.10 (2024-10-04)

### Other

* Convert OS requirement to a container requirement ([#380](https://github.com/WIPACrepo/iceprod/issues/380)) ([`f44ddf2`](https://github.com/WIPACrepo/iceprod/commit/f44ddf2b2dca023b78a2ff35c2753a598eafa5e2))

## v3.0.9 (2024-10-04)

### Other

* Add container support ([#379](https://github.com/WIPACrepo/iceprod/issues/379)) ([`b8fa632`](https://github.com/WIPACrepo/iceprod/commit/b8fa632cf984ac9273c5786d6720d7a057074dd3))

## v3.0.8 (2024-10-03)

### Other

* Set environ(OS_ARCH) ([#378](https://github.com/WIPACrepo/iceprod/issues/378)) ([`0b0012a`](https://github.com/WIPACrepo/iceprod/commit/0b0012ab55f6ab13d59ec8aa09eda0532a1632dc))

## v3.0.7 (2024-10-03)

### Other

* Fix non-standard input files ([#377](https://github.com/WIPACrepo/iceprod/issues/377)) ([`8cbdffb`](https://github.com/WIPACrepo/iceprod/commit/8cbdffb85d39a053a23806aa098113b150125db9))

## v3.0.6 (2024-10-03)

### Other

* Do gridftp env setup within try/except ([#376](https://github.com/WIPACrepo/iceprod/issues/376)) ([`b465fa5`](https://github.com/WIPACrepo/iceprod/commit/b465fa54ff90780261b8582446bb59a01b0b9258))

## v3.0.5 (2024-10-03)

### Other

* Fix unknown jobs ([#375](https://github.com/WIPACrepo/iceprod/issues/375)) ([`fd1d56b`](https://github.com/WIPACrepo/iceprod/commit/fd1d56bc773a8031973e71029ecab9509f4d9cee))

## v3.0.4 (2024-10-02)

### Other

* Reset reasons ([#374](https://github.com/WIPACrepo/iceprod/issues/374)) ([`064cf31`](https://github.com/WIPACrepo/iceprod/commit/064cf317446bfe4aee70b051801f16bb4a5fad7c))

## v3.0.3 (2024-10-02)

### Other

* <bot> update requirements.txt ([`7b2df75`](https://github.com/WIPACrepo/iceprod/commit/7b2df7548e729cae67957fa19e7e7def1d545c98))
* <bot> update requirements-tests.txt ([`4b4151c`](https://github.com/WIPACrepo/iceprod/commit/4b4151c1dcb0054c304cfcf2af6d388e572af0f0))
* <bot> update requirements-docs.txt ([`00b2d75`](https://github.com/WIPACrepo/iceprod/commit/00b2d75f1b030211120fc1ff371518e30fd722cc))

## v3.0.2 (2024-10-02)

### Other

* <bot> update requirements.txt ([`63c8841`](https://github.com/WIPACrepo/iceprod/commit/63c8841fcab1ac2fe67b8afdc26dd3f1cc71b0d5))
* <bot> update requirements-tests.txt ([`373825c`](https://github.com/WIPACrepo/iceprod/commit/373825c8380ef95640801c335b3b70ddf8372f9e))
* <bot> update requirements-docs.txt ([`b5501e5`](https://github.com/WIPACrepo/iceprod/commit/b5501e53877fe368d3b5d9a96f4aa124ddcca1f2))
* Update dataset schema to be looser ([`9197849`](https://github.com/WIPACrepo/iceprod/commit/919784994aa2b8be290b6701a38e5dd9c7efccd0))

## v3.0.1 (2024-10-01)

### Other

* Remove latest tag from cvmfs ([`4a42c8a`](https://github.com/WIPACrepo/iceprod/commit/4a42c8a3693c929b104bf526522bf3824e4a9b9e))
* Add some spare disk for stdout/err, other misc files ([`6e97f18`](https://github.com/WIPACrepo/iceprod/commit/6e97f1824a17378aa448aeaf30cd99f5b53945ac))

## v3.0.0 (2024-10-01)

### Breaking

* This release removes support for IceProd pilots, and instead relies on HTCondor file transfer and running the module code directly in the HTCondor job. As a consequence, we are able to fully support running in containers via HTCondor. ([`3c3681e`](https://github.com/WIPACrepo/iceprod/commit/3c3681e3eec318f6c41c1b81f3cb69bc28b41ab0))
* Breaking changes:     Data transfer now happens at the task level. Other file transfer is deprecated, and will be merged with the task.     Dataset config removals:         Module running_class support has been removed. Use the src attribute instead.         Data compression support has been removed.         Resources have been removed - use data in permanent input mode.         Steering/system has been removed.     The debugging pilot has limited support, and all data transfer has been removed.     The dataset truncated status has been moved to an attribute.     Tasks start at idle instead of waiting.     Task status waiting is now "ready to queue" and status queued is "on the HTCondor queue" with processing actually being processing in HTCondor.     The task reset status has been removed, and tasks will now directly go to idle. ([`3c3681e`](https://github.com/WIPACrepo/iceprod/commit/3c3681e3eec318f6c41c1b81f3cb69bc28b41ab0))

### Other

* <bot> update requirements.txt ([`71013dd`](https://github.com/WIPACrepo/iceprod/commit/71013dd43a0d9d8ea58bd358b1af5ace722ffc0a))
* <bot> update requirements-tests.txt ([`723afb6`](https://github.com/WIPACrepo/iceprod/commit/723afb61914459afbae7baab1c06ca8dd3086491))
* <bot> update requirements-docs.txt ([`3200b3a`](https://github.com/WIPACrepo/iceprod/commit/3200b3a4519f9c7b8b414f9cc636679f5eab86b0))

## v2.7.14 (2024-08-27)

### Other

* Update credentials to use new rest-tools syntax ([#373](https://github.com/WIPACrepo/iceprod/issues/373)) ([`c4bcb01`](https://github.com/WIPACrepo/iceprod/commit/c4bcb01c5590a0a32756822000aefb8d4fbd6d67))
* Fixes for materialization and job temp cleaning ([`2fddba2`](https://github.com/WIPACrepo/iceprod/commit/2fddba249db43bf3d390424120711ea5bab9f47d))

## v2.7.13 (2024-03-14)

### Other

* Update sphinx options to re-enable autodocs ([#369](https://github.com/WIPACrepo/iceprod/issues/369)) ([`a7dd319`](https://github.com/WIPACrepo/iceprod/commit/a7dd31966339327acc045cf373c0af92ba4e6c61))

## v2.7.12 (2024-02-24)

### Other

* Handle empty gridftp dirs properly ([#368](https://github.com/WIPACrepo/iceprod/issues/368)) ([`1ef6661`](https://github.com/WIPACrepo/iceprod/commit/1ef666181a233f9e278e2a35dee626e601d0921b))

## v2.7.11 (2024-01-30)

### Other

* Update docs ([#367](https://github.com/WIPACrepo/iceprod/issues/367)) ([`ab611f0`](https://github.com/WIPACrepo/iceprod/commit/ab611f0be4fd5b5f9151eb357661b01a9f3d3509))

## v2.7.10 (2023-11-08)

### Other

* <bot> update requirements.txt ([`ebf9adb`](https://github.com/WIPACrepo/iceprod/commit/ebf9adb10f0ee55db6020a7e7382275979e8aa72))
* <bot> update requirements-tests.txt ([`0f35b39`](https://github.com/WIPACrepo/iceprod/commit/0f35b390d4f769fa9c02b0b92539302a84687241))
* <bot> update requirements-docs.txt ([`246abad`](https://github.com/WIPACrepo/iceprod/commit/246abadc894f5e882c57939cff29e9ac1460cf6f))
* Fix not enough values to unpack error ([`bcf6496`](https://github.com/WIPACrepo/iceprod/commit/bcf6496baf6872069ff75e1338212ead7772b255))

## v2.7.9 (2023-11-07)

### Other

* Add condor startd hold expressions ([#366](https://github.com/WIPACrepo/iceprod/issues/366)) ([`bb37ad4`](https://github.com/WIPACrepo/iceprod/commit/bb37ad4a2a63dcebf5d763d337786627b4a1b770))

## v2.7.8 (2023-10-11)

### Other

* Update basic_submit.py fix docstring ([#365](https://github.com/WIPACrepo/iceprod/issues/365)) ([`93d4237`](https://github.com/WIPACrepo/iceprod/commit/93d423713abd91331ff569e878ef83d87b425ba0))

## v2.7.7 (2023-09-27)

### Other

* Mark cvmfs client errors as node errors ([#364](https://github.com/WIPACrepo/iceprod/issues/364)) ([`d939a44`](https://github.com/WIPACrepo/iceprod/commit/d939a447436152fd35badf55fcc33c05f455c66a))

## v2.7.6 (2023-09-25)

### Other

* Make the bias against large datasets less severe ([#363](https://github.com/WIPACrepo/iceprod/issues/363)) ([`a09e009`](https://github.com/WIPACrepo/iceprod/commit/a09e00903e2fa839f99493c5abaf55ca3d555ef0))

## v2.7.5 (2023-09-20)

### Other

* Opencl errors are node failures ([#362](https://github.com/WIPACrepo/iceprod/issues/362)) ([`7b148fc`](https://github.com/WIPACrepo/iceprod/commit/7b148fcc516ccb822be13be613819caffd941c86))

## v2.7.4 (2023-09-13)

### Other

* Fix os selection ([#361](https://github.com/WIPACrepo/iceprod/issues/361)) ([`efa52ac`](https://github.com/WIPACrepo/iceprod/commit/efa52acf9aafea60ac974d236363bddb4c3397e5))

## v2.7.3 (2023-07-26)

### Other

* Remove extra slashes in keys ([#357](https://github.com/WIPACrepo/iceprod/issues/357)) ([`9f9734c`](https://github.com/WIPACrepo/iceprod/commit/9f9734c5e1ddb20f2f9191ce806428908a8e15a6))

## v2.7.2 (2023-07-20)

### Other

* S3 site temp cleaning ([#355](https://github.com/WIPACrepo/iceprod/issues/355)) ([`cf0f029`](https://github.com/WIPACrepo/iceprod/commit/cf0f02956280b251bd6a01489527a96d4f4d4de6))

## v2.7.1 (2023-07-17)

### Other

* Fix semantic release ([`7c714aa`](https://github.com/WIPACrepo/iceprod/commit/7c714aa7988371e3d7f409adb4d828f1e32c7af5))
* Cred client ([#353](https://github.com/WIPACrepo/iceprod/issues/353)) ([`c4267b8`](https://github.com/WIPACrepo/iceprod/commit/c4267b8ecbfc51e532e5793b6533ff9776df560d))

## v2.7.0 (2023-07-10)



## v2.6.17 (2023-06-12)

### Other

* Credentials - handle refresh failures more gracefully ([#351](https://github.com/WIPACrepo/iceprod/issues/351)) ([`fcbdfe6`](https://github.com/WIPACrepo/iceprod/commit/fcbdfe603b2d2dfb1fab435e41587a576b46af58))

## v2.6.16 (2023-06-12)

### Other

* My datasets ([#350](https://github.com/WIPACrepo/iceprod/issues/350)) ([`b2a36d0`](https://github.com/WIPACrepo/iceprod/commit/b2a36d0a6d19d633df72f4b1e665b7ef33fdcd1e))

## v2.6.15 (2023-06-09)

### Other

* Only 404 if we don't match any tasks ([#349](https://github.com/WIPACrepo/iceprod/issues/349)) ([`d4700bd`](https://github.com/WIPACrepo/iceprod/commit/d4700bdfc6134a2771f12962524a25272cddc2e8))
* For names, allow all possible characters ([#348](https://github.com/WIPACrepo/iceprod/issues/348)) ([`9d84049`](https://github.com/WIPACrepo/iceprod/commit/9d84049c1174516332bb31634b0a0fd9164e0722))
* Make setting requirements in the config work, even when there are no matching tasks ([#347](https://github.com/WIPACrepo/iceprod/issues/347)) ([`91a2c81`](https://github.com/WIPACrepo/iceprod/commit/91a2c815c0e52d139f69fd41c8b96bba879f2377))

## v2.6.14 (2023-06-09)

### Other

* Priority adjustments ([#346](https://github.com/WIPACrepo/iceprod/issues/346)) ([`018a142`](https://github.com/WIPACrepo/iceprod/commit/018a1428086857baedfda92ced4198b989a3522d))

## v2.6.13 (2023-05-30)
### Other

* Bump py-versions CI release v2.1 ([#343](https://github.com/WIPACrepo/iceprod/issues/343)) ([`05e98b5`](https://github.com/WIPACrepo/iceprod/commit/05e98b50f04049f981c8760ab54ccf786aa1ff84))

## v2.6.12 (2023-04-19)
### Other
* Eliminate getip.php ([#339](https://github.com/WIPACrepo/iceprod/issues/339)) ([`6ed93a6`](https://github.com/WIPACrepo/iceprod/commit/6ed93a6aa74455b61f16dc2d6041e67a9fa4e4d5))

## v2.6.11 (2023-04-18)
### Other
* More credentials fixes ([#338](https://github.com/WIPACrepo/iceprod/issues/338)) ([`95f1a3c`](https://github.com/WIPACrepo/iceprod/commit/95f1a3c95f2939bfe3251e2aa58465c2f017b77c))

## v2.6.10 (2023-04-14)
### Other
* Fix credentials refresh post body ([#337](https://github.com/WIPACrepo/iceprod/issues/337)) ([`9a4f203`](https://github.com/WIPACrepo/iceprod/commit/9a4f203627e034142d441aa2b1bdac9ae5d84c53))

## v2.6.9 (2023-04-13)
### Other
* Py-versions syntax has changed ([#336](https://github.com/WIPACrepo/iceprod/issues/336)) ([`d9bed42`](https://github.com/WIPACrepo/iceprod/commit/d9bed42649ce6efa1bffb7dcf0a8f89fa657d71f))
* Cred API and refresh service ([`e795d2b`](https://github.com/WIPACrepo/iceprod/commit/e795d2b4b5beea16a87119abfaa0fba541bd477c))

## v2.6.8 (2023-03-03)


## v2.6.7 (2023-02-12)


## v2.6.6 (2023-02-12)


## v2.6.5 (2023-02-11)


## v2.6.4 (2023-02-11)


## v2.6.3 (2023-02-11)


## v2.6.2 (2023-02-10)


## v2.6.1 (2023-02-10)


## v2.6.0 (2023-02-09)
### Feature
* Keycloak auth ([#323](https://github.com/WIPACrepo/iceprod/issues/323)) ([`f78aff0`](https://github.com/WIPACrepo/iceprod/commit/f78aff071b84d16e437325dc2b547b609ef0deac))

## v2.5.23 (2023-02-08)


## v2.5.22 (2022-12-20)


## v2.5.21 (2022-12-18)


## v2.5.20 (2022-12-18)


## v2.5.19 (2022-12-09)


## v2.5.18 (2022-12-09)


## v2.5.17 (2022-10-28)


## v2.5.16 (2022-08-22)


## v2.5.15 (2022-08-15)


## v2.5.14 (2022-06-08)


## v2.5.13 (2022-04-13)


## v2.5.12 (2022-04-12)
### Fix
* Manual version pre-increment 2.5.6 ([`d04d749`](https://github.com/WIPACrepo/iceprod/commit/d04d749cea75a6cf6ad8e0152879d521c668968a))
* Add semantic release GH action -- will ONLY make patch w/ ([`331330c`](https://github.com/WIPACrepo/iceprod/commit/331330c91adf86f16701317be4144ec31490970d))

## v2.5.7 (2021-03-15)

