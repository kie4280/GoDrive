package watcher

import (
	"errors"
)

const (
	// RemoteChangeListSize blah
	RemoteChangeListSize = 100
	// LocalChangeListSize blah
	LocalChangeListSize = 200
)

const (
	// Moved file has only been moved or renamed
	Moved int8 = 1
	// Removed file has been removed
	Removed int8 = 2
	// Modified file has only been written to or created
	Modified int8 = 3
	// Created file is created
	Created int8 = 4
)

const (
	// C_CANCEL cancels the listAll operation
	C_CANCEL int8 = 1
	// C_GET_CHANGE queries the state
	C_GET_CHANGE int8 = 2

	// S_ACK state ACK received
	S_ACK int8 = -1
	// S_RUNNING state running
	S_RUNNING int8 = -2
	// S_NOT_READY state not ready to get changes
	S_NOT_READY int8 = -3
)

var (
	// ErrTerminated watcher terminated
	ErrTerminated = errors.New("Watcher has terminated")
	// ErrNoResponse is the error thrown when the command does return
	// ACK before timeout
	ErrNoResponse = errors.New("There is no response from the receiver")
	// ErrJammed the command channel is jammed
	ErrJammed = errors.New("The command channel is jammed")
)
