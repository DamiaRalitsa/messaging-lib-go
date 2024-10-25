package inbox

import (
	"context"
	"time"

	"github.com/DamiaRalitsa/messaging-lib-go/messaging/common"
	"github.com/go-pg/pg"
	"github.com/google/uuid"
)

type InboxManager struct {
	db      *pg.DB
	Handler common.MessageHandler
}

func NewInboxManager(db *pg.DB) *InboxManager {
	return &InboxManager{db: db}
}

func (im *InboxManager) ProcessMessage(ctx context.Context, message common.Message) error {
	return im.db.RunInTransaction(func(tx *pg.Tx) error {
		inbox, err := im.createInboxEntry(tx, message)
		if err != nil {
			return err
		}

		if err := im.Handler.Dispatch(ctx, message); err != nil {
			return err
		}

		return im.updateInboxEntry(tx, inbox)
	})
}

func (im *InboxManager) createInboxEntry(tx *pg.Tx, message common.Message) (*Inboxes, error) {
	inbox := &Inboxes{
		ID:         uuid.NewString(),
		Payload:    message.Payload,
		Topic:      message.Topic,
		IsAccepted: false,
		CreatedAt:  time.Now(),
	}

	_, err := tx.Model(inbox).Insert()
	if err != nil {
		return nil, err
	}
	return inbox, nil
}

func (im *InboxManager) updateInboxEntry(tx *pg.Tx, inbox *Inboxes) error {
	inbox.IsAccepted = true
	now := time.Now()
	inbox.ProcessedAt = &now

	_, err := tx.Model(inbox).
		Set("is_accepted = ?", inbox.IsAccepted).
		Set("processed_at = ?", inbox.ProcessedAt).
		WherePK().
		Update()
	return err
}
