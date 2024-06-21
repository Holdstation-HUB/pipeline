package task

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"github.com/Holdstation-HUB/pipeline/core"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"io"
)

const TaskTypeDecodePK core.TaskType = "decodepk"

type DecodePKTask struct {
	core.BaseTask `mapstructure:",squash"`
	Key           string `json:"private_key"`
	Secret        string `json:"secret"`
	Nonce         string `json:"nonce"`
}

var _ core.Task = (*DecodePKTask)(nil)

func (t *DecodePKTask) Type() core.TaskType {
	return TaskTypeDecodePK
}

func (t *DecodePKTask) Run(_ context.Context, _ *zap.Logger, vars core.Vars, inputs []core.Result) (core.Result, core.RunInfo) {
	var (
		secretString     core.StringParam
		privateKeyString core.StringParam
		nonceString      core.StringParam
	)
	err := multierr.Combine(
		errors.Wrap(core.ResolveParam(&privateKeyString, core.From(core.VarExpr(t.Key, vars))), "key"),
		errors.Wrap(core.ResolveParam(&nonceString, core.From(core.VarExpr(t.Nonce, vars), core.Input(inputs, 0))), "nonce"),
		errors.Wrap(core.ResolveParam(&secretString, core.From(core.VarExpr(t.Secret, vars), core.NonemptyString(t.Secret))), "secret"),
	)
	privateKeyBytes, err := hexutil.Decode(privateKeyString.String())
	if err != nil {
		return core.Result{Error: err}, core.RunInfo{}
	}

	nonceBytes, err := hexutil.Decode(nonceString.String())
	if err != nil {
		return core.Result{Error: err}, core.RunInfo{}
	}

	privateKey, err := AesGcmDecrypt([]byte(secretString.String()), privateKeyBytes, nonceBytes)
	if err != nil {
		return core.Result{Error: err}, core.RunInfo{}
	}

	pk, err := crypto.HexToECDSA(*privateKey)

	if err != nil {
		return core.Result{Error: err}, core.RunInfo{}
	}

	return core.Result{Value: pk}, core.RunInfo{}
}

// AesGcmDecrypt takes an decryption key, a ciphertext and the corresponding nonce and decrypts it with AES256 in GCM mode.
// Returns: plaintext string, error
func AesGcmDecrypt(key, ciphertext, nonce []byte) (*string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	plaintextBytes, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	plaintext := string(plaintextBytes)

	return &plaintext, nil
}

// AesGcmEncrypt takes an encryption key and a plaintext string and encrypts it with AES256 in GCM mode
// Which provides authenticated encryption. Returns the ciphertext and the used nonce.
// Return: cipherText, nonce, error
func AesGcmEncrypt(key []byte, plaintext string) ([]byte, []byte, error) {
	plaintextBytes := []byte(plaintext)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, err
	}

	// Never use more than 2^32 random nonces with a given key because of the risk of a repeat.
	nonce := make([]byte, 12)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, err
	}

	ciphertext := gcm.Seal(nil, nonce, plaintextBytes, nil)

	return ciphertext, nonce, nil
}
