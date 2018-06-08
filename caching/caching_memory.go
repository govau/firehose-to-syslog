package caching

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	cfclient "github.com/cloudfoundry-community/go-cfclient"
	uuid "github.com/satori/go.uuid"
)

type entity struct {
	Name             string                 `json:"name"`
	SpaceGUID        string                 `json:"space_guid"`
	OrganizationGUID string                 `json:"organization_guid"`
	Environment      map[string]interface{} `json:"environment_json"`
	TTL              time.Time
}

func (e *entity) appIsOptOut() bool {
	return e.Environment["F2S_DISABLE_LOGGING"] == "true"
}

type CachingMemoryConfig struct {
	IgnoreMissingApps  bool
	CacheInvalidateTTL time.Duration
}

type CachingMemory struct {
	entityCacheLock sync.Mutex
	entityCache     map[string]*entity

	client *cfclient.Client

	config *CachingMemoryConfig
}

func NewCachingMemory(client *cfclient.Client, config *CachingMemoryConfig) (*CachingMemory, error) {
	return &CachingMemory{
		entityCache: make(map[string]*entity),
		config:      config,
		client:      client,
	}, nil
}

func (cm *CachingMemory) Open() error {
	return nil
}

func (cm *CachingMemory) Close() error {
	return nil
}

func (cm *CachingMemory) readEntity(entityType, guid string) (*entity, error) {
	return nil, nil
}

// entityType *must* be checked for safety by caller
// guid will be validated as a guid by this function
func (cm *CachingMemory) getEntity(entityType, guid string) (*entity, error) {
	// First verify the GUID is in fact that - else we could become a confused deputy due to path construction issues
	u, err := uuid.FromString(guid)
	if err != nil {
		return nil, err
	}
	uuid := u.String()
	key := fmt.Sprintf("%s/%s", entityType, uuid)

	// For now, let's do a brainread mutex here. Later we can optimize...
	cm.entityCacheLock.Lock()
	defer cm.entityCacheLock.Unlock()

	// Return value if we have one
	rv, found := cm.entityCache[key]
	if found && rv.TTL.Before(time.Now()) {
		return rv, nil
	}

	// Let's fetch it
	resp, err := cm.client.DoRequestWithoutRedirects(cm.client.NewRequest(http.MethodGet, fmt.Sprintf("/v2/%s/%s", entityType, uuid)))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad status code: %s", resp.Status)
	}

	var md struct {
		Entity entity `json:"entity"`
	}
	err = json.NewDecoder(resp.Body).Decode(&md)
	if err != nil {
		return nil, err
	}

	md.Entity.TTL = time.Now().Add(cm.config.CacheInvalidateTTL)
	cm.entityCache[key] = &md.Entity

	return &md.Entity, nil
}

func (cm *CachingMemory) GetApp(appGuid string) (*App, error) {
	app, err := cm.getEntity("apps", appGuid)
	if err != nil {
		if cm.config.IgnoreMissingApps {
			app = &entity{}
		} else {
			return nil, err
		}
	}

	space, err := cm.getEntity("spaces", app.SpaceGUID)
	if err != nil {
		if cm.config.IgnoreMissingApps {
			space = &entity{}
		} else {
			return nil, err
		}
	}

	org, err := cm.getEntity("organizations", space.OrganizationGUID)
	if err != nil {
		if cm.config.IgnoreMissingApps {
			org = &entity{}
		} else {
			return nil, err
		}
	}

	return &App{
		Guid:       appGuid,
		Name:       app.Name,
		SpaceGuid:  app.SpaceGUID,
		SpaceName:  space.Name,
		OrgGuid:    space.OrganizationGUID,
		OrgName:    org.Name,
		IgnoredApp: app.appIsOptOut(),
	}, nil
}
