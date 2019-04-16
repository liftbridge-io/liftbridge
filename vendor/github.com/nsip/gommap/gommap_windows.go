// This package offers the MMap type that manipulates a memory mapped file or
// device.
//
// IMPORTANT NOTE (1): The MMap type is backed by an unsafe memory region,
// which is not covered by the normal rules of Go's memory management. If a
// slice is taken out of it, and then the memory is explicitly unmapped through
// one of the available methods, both the MMap value itself and the slice
// obtained will now silently point to invalid memory.  Attempting to access
// data in them will crash the application.

// +build windows

package gommap

import (
	"errors"
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

// The MMap type represents a memory mapped file or device. The slice offers
// direct access to the memory mapped content.
//
// IMPORTANT: Please see note in the package documentation regarding the way
// in which this type behaves.
type MMap []byte

// In order to implement 'Protect', use this to get back the original MMap properties from the memory address.
var mmapAttrs = map[uintptr]*struct {
	fd     uintptr
	offset int64
	length int64
	prot   ProtFlags
	flags  MapFlags
}{}

// GetFileSize gets the file length from its fd
func GetFileSize(fd uintptr) (int64, error) {
	fh := syscall.Handle(fd)
	fsize, err := syscall.Seek(syscall.Handle(fh), 0, 2)
	syscall.Seek(fh, 0, 0)
	return fsize, err
}

// Map creates a new mapping in the virtual address space of the calling process.
// This function will attempt to map the entire file by using the fstat system
// call with the provided file descriptor to discover its length.
func Map(fd uintptr, prot ProtFlags, flags MapFlags) (MMap, error) {
	return MapRegion(fd, 0, -1, prot, flags)
}

// MapRegion creates a new mapping in the virtual address space of the calling
// process, using the specified region of the provided file or device. If -1 is
// provided as length, this function will attempt to map until the end of the
// provided file descriptor by using the fstat system call to discover its
// length.
func MapRegion(fd uintptr, offset, length int64, prot ProtFlags, flags MapFlags) (MMap, error) {
	if offset%int64(os.Getpagesize()) != 0 {
		return nil, errors.New("offset parameter must be a multiple of the system's page size")
	}
	if length == -1 {
		length, _ = GetFileSize(fd)
	}
	/* on windows, use PROT_COPY to do the same thing as linux MAP_PRIVATE flag do */
	if flags == MAP_PRIVATE {
		prot = PROT_COPY
	}
	// return mmap(length, uintptr(prot), uintptr(flags), fd, offset)

	/*******************************/
	m, e := mmap(length, uintptr(prot), uintptr(flags), fd, offset)
	dh := (*reflect.SliceHeader)(unsafe.Pointer(&m))
	mmapAttrs[dh.Data] = &struct {
		fd     uintptr
		offset int64
		length int64
		prot   ProtFlags
		flags  MapFlags
	}{fd, offset, length, prot, flags}
	return m, e
}

func (mmap *MMap) header() *reflect.SliceHeader {
	return (*reflect.SliceHeader)(unsafe.Pointer(mmap))
}

// UnsafeUnmap deletes the memory mapped region defined by the mmap slice. This
// will also flush any remaining changes, if necessary.  Using mmap or any
// other slices based on it after this method has been called will crash the
// application.
func (mmap MMap) UnsafeUnmap() error {
	dh := mmap.header()
	return unmap(dh.Data, uintptr(dh.Len))
}

// Sync flushes changes made to the region determined by the mmap slice
// back to the device. Without calling this method, there are no guarantees
// that changes will be flushed back before the region is unmapped.  The
// flags parameter specifies whether flushing should be done synchronously
// (before the method returns) with MS_SYNC, or asynchronously (flushing is just
// scheduled) with MS_ASYNC.
func (mmap MMap) Sync(flags SyncFlags) error {
	dh := mmap.header()
	return flush(dh.Data, uintptr(dh.Len))
}

// // Advise advises the kernel about how to handle the mapped memory
// // region in terms of input/output paging within the memory region
// // defined by the mmap slice.
// func (mmap MMap) Advise(advice AdviseFlags) error {
// 	// rh := *(*reflect.SliceHeader)(unsafe.Pointer(&mmap))
// 	// _, _, err := syscall.Syscall(syscall.SYS_MADVISE, uintptr(rh.Data), uintptr(rh.Len), uintptr(advice))
// 	// if err != 0 {
// 	// 	return err
// 	// }
// 	// return nil
// }

// Protect changes the protection flags for the memory mapped region
// defined by the mmap slice.
// We use unmap & map again to implement this on windows. So can only change the protect flags on the whole
func (mmap *MMap) Protect(prot ProtFlags) (err error) {
	dh := mmap.header()
	var m MMap
	if err = mmap.UnsafeUnmap(); err == nil {
		fd, offset, length, flags := mmapAttrs[dh.Data].fd, mmapAttrs[dh.Data].offset, mmapAttrs[dh.Data].length, mmapAttrs[dh.Data].flags
		mmapAttrs[dh.Data] = nil
		if m, err = MapRegion(fd, offset, length, prot, flags); err == nil {
			mmap = &m
		}
	}
	return
}

// Lock locks the mapped region defined by the mmap slice,
// preventing it from being swapped out.
func (mmap MMap) Lock() error {
	dh := mmap.header()
	return lock(dh.Data, uintptr(dh.Len))
}

// Unlock unlocks the mapped region defined by the mmap slice,
// allowing it to swap out again.
func (mmap MMap) Unlock() error {
	dh := mmap.header()
	return unlock(dh.Data, uintptr(dh.Len))
}

// // IsResident returns a slice of booleans informing whether the respective
// // memory page in mmap was mapped at the time the call was made.
// func (mmap MMap) IsResident() ([]bool, error) {
// 	pageSize := os.Getpagesize()
// 	result := make([]bool, (len(mmap)+pageSize-1)/pageSize)
// 	rh := *(*reflect.SliceHeader)(unsafe.Pointer(&mmap))
// 	resulth := *(*reflect.SliceHeader)(unsafe.Pointer(&result))
// 	_, _, err := syscall.Syscall(syscall.SYS_MINCORE, uintptr(rh.Data), uintptr(rh.Len), uintptr(resulth.Data))
// 	for i := range result {
// 		*(*uint8)(unsafe.Pointer(&result[i])) &= 1
// 	}
// 	if err != 0 {
// 		return nil, err
// 	}
// 	return result, nil
// }
