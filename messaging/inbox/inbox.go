package inbox

import (
	"encoding/json"
	"time"
)

type Inboxes struct {
	ID          string          `pg:"id,type:uuid,default:uuid_generate_v4(),pk"`
	Payload     json.RawMessage `pg:"type:jsonb,notnull"`
	Topic       string          `pg:"type:varchar(255),notnull"`
	IsAccepted  bool            `pg:"type:bool,notnull,default:false"`
	Publisher   *string         `pg:"type:varchar(255),default:null"`
	CreatedAt   time.Time       `pg:"type:timestamptz,default:now()"`
	CreatedBy   string          `pg:"type:varchar(255)"`
	UpdatedAt   time.Time       `pg:"type:timestamptz,default:now()"`
	UpdatedBy   string          `pg:"type:varchar(255)"`
	ProcessedAt *time.Time      `pg:"type:timestamptz,default:null"`
}
