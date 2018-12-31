
## This is the original text from the upstream readme, preserved for reference:

gommap
======

This is a git mirror of [launchpad.net/gommap][bzr_source]. The `master` branch
includes [this patch][osx_patch], which adds support for darwin64 (MacOS X).

[bzr_source]: http://launchpad.net/gommap
[osx_patch]: https://code.launchpad.net/~karl-excors/gommap/gommap-darwin64/+merge/129364

[Read more](http://labix.org/gommap)

[API documentation](http://godoc.org/github.com/tysontate/gommap)


nsip changes
============

This fork was created so that we could add windows support to gommap.

Gommap is a dependency within the [liftbridge](https://github.com/liftbridge-io/liftbridge) streaming engine which we are using, and we need to be able to run the streaming server on windows.

The Gommap windows implementation was done by Qing Miao from the NSIP team, but we drew heavily on work from others which are referenced below to make sure they get the credit they deserve - it's also ended up being a bit of a sample of known golang memory maps that work cross-platform, so putting it here in case others find it useful.

The state of many implementations of the mem-maps is unclear in terms of support, how active the repos are etc. Of course this is a low-level piece of any system so probably once people have one working they don't do any changes until an OS or processor architecture change forces the need.

We created this fork really because we're prepared to support the implementation for now, and we're passing the changes back up to the original repo so they can accept them if they want to.

The windows version is designed to require no changes to existing code; certainly this is the case for our liftbridge requirement, the liftbridge server now builds and runs fine on windows, and it's been subjected to high volume tests that would force the flushing of the map without incident.

### Limitations
1. We have only tested this on 64-bit Windows
1. We have not been able to implement the (mmap)Advise() or (mmap)IsResident() functions - later versions of windows may have apis that can help to support these (from the documentation we've been able to find), but go can only distinguish os and architecture and those 'advanced' features are not generically available to 'windows'. Please raise an issue if this is a show-stopper.


### Prior Art
Here’s a list of the alternative go mem-map packages that we looked at for help and/or borrowed from directly, and which anyone else may find helpful if you need cross-platform memory-mapping in go:

https://github.com/edsrzf/mmap-go
This one has a very similar interface to the original labix library, but does seem to provide support for windows and various linux distros and Mac.

https://github.com/golang/exp/tree/master/mmap
Package from the golang devs for mmap. Not immediately useful for our requirement as it only offers a reader interface, but does have nice use of the syscall package in the windows golang file that might be useful if we end up having to create our own library completely from scratch.

https://github.com/justmao945/tama/tree/master/mmap
This one uses a full go File paradigm rather than a byte array. 

https://github.com/influxdata/platform/tree/master/pkg/mmap
This one is used by influxdb, so we know it works on multiple platforms (we also use influxdb as part of the same project). Difference here is that the API is much simpler, just open - returns a byte array, and then close!

https://github.com/go-util/mmap
An interesting one that is actively developed. Uses a mixture of file/byte array paradigms, so may operate on Windows using a file-based approach, with lower-level calls for unix systems; offers reader and writer interfaces if required.

https://github.com/AlexStocks/goext/tree/master/syscall
Another active repo, with a mmap for unix and windows, offers the simple interface for byte array which should be compatible with the simple calls used by liftbridge.


