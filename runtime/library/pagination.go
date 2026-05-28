package library

import (
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// paginateList applies limit+continue pagination to a full list of items.
// It sorts items by name for deterministic ordering, validates any continue
// token against the current RV, and returns the page with appropriate
// ListMeta (continue token, remainingItemCount).
//
// If limit is 0 and continueToken is empty, items are returned unchanged.
// Returns 410 Gone if the continue token's RV doesn't match currentRV.
// Returns 400 BadRequest if the continue token is malformed.
func paginateList(list runtime.Object, limit int64, continueToken, currentRV string) error {
	if limit == 0 && continueToken == "" {
		return nil
	}

	items, err := meta.ExtractList(list)
	if err != nil {
		return fmt.Errorf("extracting list items: %w", err)
	}

	// Sort by name for deterministic ordering.
	sort.Slice(items, func(i, j int) bool {
		ai, _ := meta.Accessor(items[i])
		aj, _ := meta.Accessor(items[j])
		if ai == nil || aj == nil {
			return false
		}
		return ai.GetName() < aj.GetName()
	})

	// Determine offset from continue token.
	offset := 0
	if continueToken != "" {
		tokenRV, tokenOffset, decErr := decodeContinueToken(continueToken)
		if decErr != nil {
			return apierrors.NewBadRequest(fmt.Sprintf("invalid continue token: %v", decErr))
		}
		if tokenRV != currentRV {
			return apierrors.NewResourceExpired(fmt.Sprintf(
				"the provided continue token is expired: resource version changed from %s to %s",
				tokenRV, currentRV))
		}
		offset = tokenOffset
	}

	// Apply offset and limit.
	total := len(items)
	if offset > total {
		offset = total
	}
	end := total
	if limit > 0 && offset+int(limit) < total {
		end = offset + int(limit)
	}
	page := items[offset:end]

	// Build the continue token for the next page.
	var nextContinue string
	if end < total {
		nextContinue = encodeContinueToken(currentRV, end)
	}

	// Set items on the list.
	if err := meta.SetList(list, page); err != nil {
		return fmt.Errorf("setting list items: %w", err)
	}

	// Update list metadata.
	if li, ok := list.(metav1.ListInterface); ok {
		li.SetResourceVersion(currentRV)
		li.SetContinue(nextContinue)
		if end < total {
			remaining := int64(total - end)
			li.SetRemainingItemCount(&remaining)
		} else {
			li.SetRemainingItemCount(nil)
		}
	}

	return nil
}

// encodeContinueToken encodes rv and offset into a base64 token.
// Format: base64("rv:offset")
func encodeContinueToken(rv string, offset int) string {
	raw := fmt.Sprintf("%s:%d", rv, offset)
	return base64.StdEncoding.EncodeToString([]byte(raw))
}

// decodeContinueToken decodes a continue token into rv and offset.
func decodeContinueToken(token string) (rv string, offset int, err error) {
	raw, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return "", 0, fmt.Errorf("base64 decode: %w", err)
	}
	parts := strings.SplitN(string(raw), ":", 2)
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("expected rv:offset format, got %q", string(raw))
	}
	offset, err = strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, fmt.Errorf("invalid offset %q: %w", parts[1], err)
	}
	return parts[0], offset, nil
}
