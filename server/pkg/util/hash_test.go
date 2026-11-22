package util

import "testing"

func TestBcryptHashAndCheck(t *testing.T) {
	hash, err := BcryptHash("correct horse battery staple")
	if err != nil {
		t.Fatalf("BcryptHash() error: %v", err)
	}
	if hash == "" {
		t.Fatal("BcryptHash() returned empty hash")
	}
	if !BcryptCheck("correct horse battery staple", hash) {
		t.Fatal("BcryptCheck() rejected correct password")
	}
	if BcryptCheck("wrong", hash) {
		t.Fatal("BcryptCheck() accepted wrong password")
	}
}
