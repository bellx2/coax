// Copyright 2026 Till0196
// SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

package pool

import (
	"github.com/Till0196/coax/internal/coaxd/device"
	"github.com/Till0196/coax/internal/winscard/usbcard"
)

// Cards presents the pool as the source of boards the card provider draws on. A
// PX-W3U4's card slot and its four tuners are one USB device whose interface can
// be claimed once, so a provider opening boards of its own would fight the
// recordings for the same handle. Here a card session is one more reason to keep
// a board open, counted beside the tuners.
func (s *Supervisor) Cards() usbcard.BoardSource { return cardSource{pool: s} }

type cardSource struct{ pool *Supervisor }

// Boards lists what the card provider may find a slot on. Nothing is opened.
func (c cardSource) Boards() ([]usbcard.Attached, error) {
	catalog, err := c.pool.refresh()
	if err != nil {
		return nil, err
	}
	boards := make([]usbcard.Attached, 0, len(catalog))
	for _, one := range catalog {
		boards = append(boards, usbcard.Attached{
			Backend:  one.detected.Backend,
			Selector: one.detected.Selector,
			ID:       one.detected.USB,
			Serial:   one.detected.Serial,
			Port:     one.detected.Port,
		})
	}
	return boards, nil
}

// Open lends the card provider the board a slot is in. The hold is the card's
// own: the board stays open while the provider has it whether or not a tuner is
// running, and letting go does not close it under a recording.
func (c cardSource) Open(backend, selector string) (usbcard.Borrow, error) {
	catalog, err := c.pool.refresh()
	if err != nil {
		return usbcard.Borrow{}, err
	}
	key := Key{Backend: backend, Selector: selector}
	for _, one := range catalog {
		if one.key != key {
			continue
		}
		held, err := c.pool.board(one)
		if err != nil {
			return usbcard.Borrow{}, err
		}
		hold, err := held.card(Owner{})
		if err != nil {
			return usbcard.Borrow{}, err
		}
		held.mutex.Lock()
		opened := held.opened
		held.mutex.Unlock()
		if opened == nil {
			hold.Release()
			return usbcard.Borrow{}, ErrGone
		}
		return usbcard.Borrow{
			Board: failingCard{Board: opened, reader: hold.Reader(), board: held},
			Identity: usbcard.Identity{
				Serial:     one.detected.Serial,
				Port:       one.detected.Port,
				Generation: hold.Generation(),
			},
			// Answered from the generation the board already keeps, so asking
			// before every card exchange costs nothing.
			Valid:   func() bool { return !hold.Stale() },
			Release: hold.Release,
		}, nil
	}
	return usbcard.Borrow{}, ErrUnknownTuner
}

// failingCard hands the card's failures to the board, as a tuner's are. A board
// that dropped off the bus and came back in the same socket keeps its key, so
// the listing never says it went; a card session holding it would otherwise
// talk to the dead handle for as long as the daemon runs. Marked gone, the
// board is reopened on the next borrow.
type failingCard struct {
	device.Board
	reader device.CardReader
	board  *board
}

func (c failingCard) CardStatus() (device.CardStatus, error) {
	status, err := c.reader.CardStatus()
	return status, c.board.fail(err)
}

func (c failingCard) ResetCard() ([]byte, error) {
	atr, err := c.reader.ResetCard()
	return atr, c.board.fail(err)
}

func (c failingCard) TransmitAPDU(apdu []byte) ([]byte, error) {
	response, err := c.reader.TransmitAPDU(apdu)
	return response, c.board.fail(err)
}
