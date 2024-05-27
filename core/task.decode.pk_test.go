package core

import (
	"context"
	"crypto/ecdsa"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/magiconair/properties/assert"
	"pipeline/test"
	"testing"
)

var passcode = "e16a05e6aa935e5f3b53483750b80308"

func TestDecodePrivateKey(t *testing.T) {
	privateKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	privateKeyBytes := crypto.FromECDSA(privateKey)
	privKey := hexutil.Encode(privateKeyBytes)[2:]
	encodedPrivateKey, nonce, err := AesGcmEncrypt([]byte(passcode), privKey)
	if err != nil {
		t.Fatal(err)
	}

	zapLog := test.NewMockZapLog()
	db, _ := test.NewMockDB()
	runner := NewRunner(NewDefaultConfig(), zapLog, nil, nil, db)

	specs := Spec{
		DotDagSource: `
			private_key_decoded [type="decodepk" key="$(wallet.private_key)" secret="$(wallet.secret)" nonce="$(wallet.nonce)"]
		`,
	}
	params := map[string]interface{}{
		"wallet": map[string]interface{}{
			"private_key": hexutil.Encode(encodedPrivateKey),
			"nonce":       hexutil.Encode(nonce),
			"secret":      passcode,
		},
	}
	_, trrs, err := runner.ExecuteRun(context.TODO(), specs, NewVarsFrom(params), zapLog)
	if err != nil {
		t.Fatal(err)
	}

	finalResult, err := trrs.FinalResult(nil).SingularResult()
	if err != nil {
		t.Fatal(err)
	}

	decodedPrivateKey := finalResult.Value.(*ecdsa.PrivateKey)
	assert.Equal(t, privateKey, decodedPrivateKey)
}
