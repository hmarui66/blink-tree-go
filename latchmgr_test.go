package main

import (
	"testing"
	"time"
)

func TestBLTRWLock_WriteLockAndRelease(t *testing.T) {
	type fields struct {
		rin     uint32
		rout    uint32
		ticket  uint32
		serving uint32
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "write lock",
			fields: fields{
				rin:     0,
				rout:    0,
				ticket:  0,
				serving: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lock := &BLTRWLock{
				rin:     tt.fields.rin,
				rout:    tt.fields.rout,
				ticket:  tt.fields.ticket,
				serving: tt.fields.serving,
			}
			lock.WriteLock()

			if lock.rin != 2 {
				t.Errorf("rin = %d, want 2", lock.rin)
			}
			if lock.rout != 0 {
				t.Errorf("rout = %d, want 0", lock.rout)
			}
			if lock.ticket != 1 {
				t.Errorf("ticket = %d, want 1", lock.ticket)
			}
			if lock.serving != 0 {
				t.Errorf("serving = %d, want 0", lock.serving)
			}

			lock.WriteRelease()
			if lock.rin != 0 {
				t.Errorf("rin = %d, want 0", lock.rin)
			}
			if lock.rout != 0 {
				t.Errorf("rout = %d, want 0", lock.rout)
			}
			if lock.ticket != 1 {
				t.Errorf("ticket = %d, want 1", lock.ticket)
			}
			if lock.serving != 1 {
				t.Errorf("serving = %d, want 1", lock.serving)
			}
		})
	}
}

func TestBLTRWLock_ReadLockAndRelease(t *testing.T) {
	type fields struct {
		rin     uint32
		rout    uint32
		ticket  uint32
		serving uint32
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "read lock",
			fields: fields{
				rin:     0,
				rout:    0,
				ticket:  0,
				serving: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lock := &BLTRWLock{
				rin:     tt.fields.rin,
				rout:    tt.fields.rout,
				ticket:  tt.fields.ticket,
				serving: tt.fields.serving,
			}
			lock.ReadLock()

			if lock.rin != 4 {
				t.Errorf("rin = %d, want 4", lock.rin)
			}
			if lock.rout != 0 {
				t.Errorf("rout = %d, want 0", lock.rout)
			}
			if lock.ticket != 0 {
				t.Errorf("ticket = %d, want 0", lock.ticket)
			}
			if lock.serving != 0 {
				t.Errorf("serving = %d, want 0", lock.serving)
			}

			lock.ReadRelease()
			if lock.rin != 4 {
				t.Errorf("rin = %d, want 4", lock.rin)
			}
			if lock.rout != 4 {
				t.Errorf("rout = %d, want 4", lock.rout)
			}
			if lock.ticket != 0 {
				t.Errorf("ticket = %d, want 0", lock.ticket)
			}
			if lock.serving != 0 {
				t.Errorf("serving = %d, want 0", lock.serving)
			}
		})
	}
}

func TestBLTRWLock_ReadAndWriteLock(t *testing.T) {
	lock := &BLTRWLock{
		rin:     0,
		rout:    0,
		ticket:  0,
		serving: 0,
	}

	t.Run("read and write lock", func(t *testing.T) {
		start := time.Now()
		lock.ReadLock()
		t.Logf("ReadLock after %v", time.Since(start))
		// ReadRelease after 1 sec
		go func() {
			time.Sleep(300 * time.Millisecond)
			t.Logf("ReadRelease after %v", time.Since(start))
			lock.ReadRelease()
		}()

		lock.WriteLock()
		t.Logf("WriteLock after %v", time.Since(start))

		if lock.rin != 6 {
			t.Errorf("rin = %d, want 6", lock.rin)
		}
		if lock.rout != 4 {
			t.Errorf("rout = %d, want 4", lock.rout)
		}
		if lock.ticket != 1 {
			t.Errorf("ticket = %d, want 1", lock.ticket)
		}
		if lock.serving != 0 {
			t.Errorf("serving = %d, want 0", lock.serving)
		}
	})
}

func TestBLTRWLock_ReadAndWriteAndReadLock(t *testing.T) {
	lock := &BLTRWLock{
		rin:     0,
		rout:    0,
		ticket:  0,
		serving: 0,
	}

	t.Run("read and write and read lock", func(t *testing.T) {
		start := time.Now()
		lock.ReadLock()
		t.Logf("ReadLock1 after %v", time.Since(start))
		// ReadRelease after 1 sec
		go func() {
			time.Sleep(300 * time.Millisecond)
			t.Logf("ReadRelease after %v", time.Since(start))
			lock.ReadRelease()
		}()

		go func() {
			lock.WriteLock()
			t.Logf("WriteLock after %v", time.Since(start))
			go func() {
				time.Sleep(300 * time.Millisecond)
				t.Logf("WriteRelease after %v", time.Since(start))
				lock.WriteRelease()
			}()
		}()

		time.Sleep(100 * time.Millisecond)
		lock.ReadLock()
		t.Logf("ReadLock2 after %v", time.Since(start))

		if lock.rin != 8 {
			t.Errorf("rin = %d, want 8", lock.rin)
		}
		if lock.rout != 4 {
			t.Errorf("rout = %d, want 4", lock.rout)
		}
		if lock.ticket != 1 {
			t.Errorf("ticket = %d, want 1", lock.ticket)
		}
		if lock.serving != 1 {
			t.Errorf("serving = %d, want 1", lock.serving)
		}
	})
}

func TestBLTRWLock_WriteAndReadLock(t *testing.T) {
	lock := &BLTRWLock{
		rin:     0,
		rout:    0,
		ticket:  0,
		serving: 0,
	}

	t.Run("write and read lock", func(t *testing.T) {
		start := time.Now()
		lock.WriteLock()
		t.Logf("WriteLock after %v", time.Since(start))
		// ReadRelease after 1 sec
		go func() {
			time.Sleep(1 * time.Second)
			t.Logf("WriteRelease after %v", time.Since(start))
			lock.WriteRelease()
		}()

		lock.ReadLock()
		t.Logf("ReadLock after %v", time.Since(start))

		if lock.rin != 4 {
			t.Errorf("rin = %d, want 4", lock.rin)
		}
		if lock.rout != 0 {
			t.Errorf("rout = %d, want 0", lock.rout)
		}
		if lock.ticket != 1 {
			t.Errorf("ticket = %d, want 1", lock.ticket)
		}
		if lock.serving != 1 {
			t.Errorf("serving = %d, want 1", lock.serving)
		}
	})
}

func TestBLTRWLock_ReadAndReadLock(t *testing.T) {
	lock := &BLTRWLock{
		rin:     0,
		rout:    0,
		ticket:  0,
		serving: 0,
	}

	t.Run("read and read lock", func(t *testing.T) {
		start := time.Now()
		lock.ReadLock()
		t.Logf("ReadLock after %v", time.Since(start))

		lock.ReadLock()
		t.Logf("ReadLock after %v", time.Since(start))

		if lock.rin != 8 {
			t.Errorf("rin = %d, want 8", lock.rin)
		}
		if lock.rout != 0 {
			t.Errorf("rout = %d, want 0", lock.rout)
		}
		if lock.ticket != 0 {
			t.Errorf("ticket = %d, want 0", lock.ticket)
		}
		if lock.serving != 0 {
			t.Errorf("serving = %d, want 0", lock.serving)
		}
	})
}

func TestBLTRWLock_WriteAndWriteLock(t *testing.T) {
	lock := &BLTRWLock{
		rin:     0,
		rout:    0,
		ticket:  0,
		serving: 0,
	}

	t.Run("write and write lock", func(t *testing.T) {
		start := time.Now()
		lock.WriteLock()
		t.Logf("WriteLock after %v", time.Since(start))
		// ReadRelease after 1 sec
		go func() {
			time.Sleep(1 * time.Second)
			t.Logf("WriteRelease after %v", time.Since(start))
			lock.WriteRelease()
		}()

		lock.WriteLock()
		t.Logf("WriteLock after %v", time.Since(start))

		if lock.rin != 3 {
			t.Errorf("rin = %d, want 3", lock.rin)
		}
		if lock.rout != 0 {
			t.Errorf("rout = %d, want 0", lock.rout)
		}
		if lock.ticket != 2 {
			t.Errorf("ticket = %d, want 2", lock.ticket)
		}
		if lock.serving != 1 {
			t.Errorf("serving = %d, want 1", lock.serving)
		}
	})
}
