package googleclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"godrive/internal/utils"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"io/ioutil"
	"net/http"
	"os"
)

// Exported errors
var (
	//ErrReadSecret permission error or something
	ErrReadSecret error = errors.New("Unable to read client secret file")
	// ErrUserAuthCodeError stdin error
	ErrUserAuthCodeError error = errors.New("Unable to read authorization code")
	// ErrCacheOauth write error maybe?
	ErrCacheOauth error = errors.New("Unable to cache oauth token")
	// ErrParseError parse json error
	ErrParseError error = errors.New("Unable to parse client secret file to config")
	// ErrAuthWebCode invalid code probably?
	ErrAuthWebCode error = errors.New("Unable to retrieve token from web")
)

var (
	userServices map[string]*drive.Service = make(map[string]*drive.Service)
)

// Retrieve a token, saves the token, then returns the generated client.
func getClient(id string, config *oauth2.Config) (*http.Client, error) {
	// The file token.json stores the user's access and refresh tokens, and is
	// created automatically when the authorization flow completes for the first
	// time.

	tokFile := "./secrets/token_" + id + ".json"
	tok, err := tokenFromFile(tokFile)
	if err != nil {
		if os.IsNotExist(err) {
			tok, err = getTokenFromWeb(config)
			if err != nil {
				return nil, err
			}
			err = saveToken(tokFile, tok)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	return config.Client(context.Background(), tok), nil
}

// Request a token from the web, then returns the retrieved token.
func getTokenFromWeb(config *oauth2.Config) (*oauth2.Token, error) {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the "+
		"authorization code: \n%v\n", authURL)

	var authCode string
	_, err := fmt.Scan(&authCode)
	if err != nil {
		return nil, utils.NewError(ErrUserAuthCodeError, err)

	}

	tok, err := config.Exchange(context.TODO(), authCode)
	if err != nil {
		return nil, utils.NewError(ErrAuthWebCode, err)

	}
	return tok, nil
}

// Retrieves a token from a local file.
func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)

	if err != nil {
		return nil, err
	}
	defer f.Close()
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}

// Saves a token to a file path.
func saveToken(path string, token *oauth2.Token) error {
	fmt.Printf("Saving credential file to: %s\n", path)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return utils.NewError(ErrCacheOauth, err)
	}
	defer f.Close()
	err = json.NewEncoder(f).Encode(token)

	return err
}

// NewService creates a drive client
func NewService(id string) (*drive.Service, error) {

	cc, ok := userServices[id]
	if ok {
		return cc, nil
	}

	b, err := ioutil.ReadFile("./secrets/client_secret_1.json")
	if err != nil {
		return nil, utils.NewError(ErrReadSecret, err)
	}

	// If modifying these scopes, delete your previously saved token.json.
	config, err := google.ConfigFromJSON(b, drive.DriveScope)
	if err != nil {
		return nil, utils.NewError(ErrParseError, err)
	}
	client, err := getClient(id, config)
	if err != nil {
		return nil, err
	}
	srv, err := drive.New(client)
	if err != nil {
		userServices[id] = srv
	}

	return srv, err
}
