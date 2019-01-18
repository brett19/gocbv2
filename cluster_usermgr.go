package gocb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"strings"

	"gopkg.in/couchbase/gocbcore.v7"
)

// UserManager provides methods for performing Couchbase user management.
type UserManager struct {
	httpClient httpProvider
}

// UserRole represents a role for a particular user on the server.
type UserRole struct {
	Role       string
	BucketName string
}

// User represents a user which was retrieved from the server.
type User struct {
	Id    string
	Name  string
	Type  string
	Roles []UserRole
}

// AuthDomain specifies the user domain of a specific user
type AuthDomain string

const (
	// LocalDomain specifies users that are locally stored in Couchbase.
	LocalDomain AuthDomain = "local"

	// ExternalDomain specifies users that are externally stored
	// (in LDAP for instance).
	ExternalDomain = "external"
)

// UserSettings represents a user during user creation.
type UserSettings struct {
	Name     string
	Password string
	Roles    []UserRole
}

type userRoleJson struct {
	Role       string `json:"role"`
	BucketName string `json:"bucket_name"`
}

type userJson struct {
	Id    string         `json:"id"`
	Name  string         `json:"name"`
	Type  string         `json:"type"`
	Roles []userRoleJson `json:"roles"`
}

type userSettingsJson struct {
	Name     string         `json:"name"`
	Password string         `json:"password"`
	Roles    []userRoleJson `json:"roles"`
}

func transformUserJson(userData *userJson) User {
	var user User
	user.Id = userData.Id
	user.Name = userData.Name
	user.Type = userData.Type
	for _, roleData := range userData.Roles {
		user.Roles = append(user.Roles, UserRole{
			Role:       roleData.Role,
			BucketName: roleData.BucketName,
		})
	}
	return user
}

// GetUsers returns a list of all users on the cluster.
func (um *UserManager) GetUsers(domain AuthDomain) ([]*User, error) {
	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(MgmtService),
		Method:  "GET",
		Path:    fmt.Sprintf("/settings/rbac/users/%s", domain),
	}

	resp, err := um.httpClient.DoHttpRequest(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return nil, networkError{statusCode: resp.StatusCode, message: string(data)}
	}

	var usersData []*userJson
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&usersData)
	if err != nil {
		return nil, err
	}

	var users []*User
	for _, userData := range usersData {
		user := transformUserJson(userData)
		users = append(users, &user)
	}

	return users, nil
}

// GetUser returns the data for a particular user
func (um *UserManager) GetUser(domain AuthDomain, name string) (*User, error) {
	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(MgmtService),
		Method:  "GET",
		Path:    fmt.Sprintf("/settings/rbac/users/%s/%s", domain, name),
	}

	resp, err := um.httpClient.DoHttpRequest(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return nil, networkError{statusCode: resp.StatusCode, message: string(data)}
	}

	var userData userJson
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&userData)
	if err != nil {
		return nil, err
	}

	user := transformUserJson(&userData)
	return &user, nil
}

// UpsertUser updates a built-in RBAC user on the cluster.
func (um *UserManager) UpsertUser(domain AuthDomain, name string, settings *UserSettings) error {
	var reqRoleStrs []string
	for _, roleData := range settings.Roles {
		reqRoleStrs = append(reqRoleStrs, fmt.Sprintf("%s[%s]", roleData.Role, roleData.BucketName))
	}

	reqForm := make(url.Values)
	reqForm.Add("name", settings.Name)
	reqForm.Add("password", settings.Password)
	reqForm.Add("roles", strings.Join(reqRoleStrs, ","))

	req := &gocbcore.HttpRequest{
		Service:     gocbcore.ServiceType(MgmtService),
		Method:      "PUT",
		Path:        fmt.Sprintf("/settings/rbac/users/%s/%s", domain, name),
		Body:        []byte(reqForm.Encode()),
		ContentType: "application/x-www-form-urlencoded",
	}

	resp, err := um.httpClient.DoHttpRequest(req)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return networkError{statusCode: resp.StatusCode, message: string(data)}
	}

	return nil
}

// RemoveUser removes a built-in RBAC user on the cluster.
func (um *UserManager) RemoveUser(domain AuthDomain, name string) error {
	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(MgmtService),
		Method:  "DELETE",
		Path:    fmt.Sprintf("/settings/rbac/users/%s/%s", domain, name),
	}

	resp, err := um.httpClient.DoHttpRequest(req)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return networkError{statusCode: resp.StatusCode, message: string(data)}
	}

	return nil
}
