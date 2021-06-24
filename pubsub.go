package pubsubtools

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/savannahghi/converterandformatter"
	"github.com/savannahghi/serverutils"
	"google.golang.org/api/idtoken"
	"google.golang.org/api/iterator"
)

// pubsub constants
const (
	PubSubHandlerPath = "/pubsub"
	// TODO: make this Env Vars
	Aud = "bewell.co.ke"

	authHeaderName     = "Authorization"
	googleIss          = "accounts.google.com"
	googleAccountsIss  = "https://accounts.google.com"
	topicKey           = "topicID"
	ackDeadlineSeconds = 60
	maxBackoffSeconds  = 600
	hoursInAWeek       = 24 * 7
)

// PubSubMessage is a pub-sub message payload.
//
// See https://cloud.google.com/pubsub/docs/push for more context.
//
// The message that is POSTed looks like the example below:
//
// {
//     "message": {
//         "attributes": {
//             "key": "value"
//         },
//         "data": "SGVsbG8gQ2xvdWQgUHViL1N1YiEgSGVyZSBpcyBteSBtZXNzYWdlIQ==",
//         "messageId": "136969346945"
//     },
//    "subscription": "projects/myproject/subscriptions/mysubscription"
// }
type PubSubMessage struct {
	MessageID  string            `json:"messageId"`
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes"`
}

// PubSubPayload is the payload of a Pub/Sub event.
type PubSubPayload struct {
	Message      PubSubMessage `json:"message"`
	Subscription string        `json:"subscription"`
}

// VerifyPubSubJWTAndDecodePayload confirms that there is a valid Google signed
// JWT and decodes the pubsub message payload into a struct.
//
// It's use will simplify & shorten the handler funcs that process Cloud Pubsub
// push notifications.
func VerifyPubSubJWTAndDecodePayload(
	w http.ResponseWriter,
	r *http.Request,
) (*PubSubPayload, error) {
	authHeader := r.Header.Get(authHeaderName)
	if authHeader == "" || len(strings.Split(authHeader, " ")) != 2 {
		return nil, fmt.Errorf("missing Authorization Header")
	}

	token := strings.Split(authHeader, " ")[1]
	payload, err := idtoken.Validate(
		r.Context(), token, Aud)
	if err != nil {
		return nil, fmt.Errorf("invalid token: %w", err)
	}
	if payload.Issuer != googleIss && payload.Issuer != googleAccountsIss {
		return nil, fmt.Errorf("%s is not a valid issuer", payload.Issuer)
	}

	// decode the message
	var m PubSubPayload
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("can't read request body: %w", err)
	}

	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf(
			"can't unmarshal payload body into JSON struct: %w", err)
	}

	// return the decoded message if there is no error
	return &m, nil
}

// GetPubSubTopic retrieves a pubsub topic from a pubsub payload.
//
// It follows a convention where the topic is sent as an attribute under the
// `topicID` key.
func GetPubSubTopic(m *PubSubPayload) (string, error) {
	if m == nil {
		return "", fmt.Errorf("nil pub sub payload")
	}
	attrs := m.Message.Attributes
	topicID, prs := m.Message.Attributes[topicKey]
	if !prs {
		return "", fmt.Errorf(
			"no `%s` key in message attributes %#v", topicKey, attrs)
	}
	return topicID, nil
}

// EnsureTopicsExist creates the topic(s) in the suppplied list if they do not
// already exist.
func EnsureTopicsExist(
	ctx context.Context,
	pubsubClient *pubsub.Client,
	topicIDs []string,
) error {
	if pubsubClient == nil {
		return fmt.Errorf("nil pubsub client")
	}

	// get a list of configured topic IDs from the project so that we don't recreate
	configuredTopics := []string{}
	it := pubsubClient.Topics(ctx)
	for {
		topic, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf(
				"error while iterating through pubsub topics: %w", err)
		}
		configuredTopics = append(configuredTopics, topic.ID())
	}

	// ensure that all our desired topics are all created
	for _, topicID := range topicIDs {
		if !converterandformatter.StringSliceContains(configuredTopics, topicID) {
			_, err := pubsubClient.CreateTopic(ctx, topicID)
			if err != nil {
				return fmt.Errorf("can't create topic %s: %w", topicID, err)
			}
		}
	}

	return nil
}

// EnsureSubscriptionsExist ensures that the subscriptions named in the supplied
// topic:subscription map exist. If any does not exist, it is created.
func EnsureSubscriptionsExist(
	ctx context.Context,
	pubsubClient *pubsub.Client,
	topicSubscriptionMap map[string]string,
	callbackURL string,
) error {
	if pubsubClient == nil {
		return fmt.Errorf("nil pubsub client")
	}

	for topicID, subscriptionID := range topicSubscriptionMap {
		topic := pubsubClient.Topic(topicID)
		topicExists, err := topic.Exists(ctx)
		if err != nil {
			return fmt.Errorf("error when checking if topic %s exists: %w", topicID, err)
		}

		if !topicExists {
			return fmt.Errorf("no topic with ID %s exists", topicID)
		}

		subscriptionConfig, err := GetPushSubscriptionConfig(
			ctx,
			pubsubClient,
			topicID,
			callbackURL,
		)
		if err != nil {
			return fmt.Errorf(
				"can't initialize subscription config for topic %s: %w", topicID, err)
		}

		if subscriptionConfig == nil {
			return fmt.Errorf("nil subscription config")
		}

		subscriptionExists, err := pubsubClient.Subscription(subscriptionID).Exists(ctx)
		if err != nil {
			return fmt.Errorf("error when checking if a subscription exists: %w", err)
		}
		if !subscriptionExists {
			sub, err := pubsubClient.CreateSubscription(ctx, subscriptionID, *subscriptionConfig)
			if err != nil {
				log.Printf("Detailed error:\n%#v\n", err)
				return fmt.Errorf("can't create subscription %s: %w", topicID, err)
			}
			log.Printf("created subscription %#v with config %#v", sub, *subscriptionConfig)
		}
	}

	return nil
}

// GetPushSubscriptionConfig creates a push subscription configuration with the
// supplied parameters.
func GetPushSubscriptionConfig(
	ctx context.Context,
	pubsubClient *pubsub.Client,
	topicID string,
	callbackURL string,
) (*pubsub.SubscriptionConfig, error) {
	if pubsubClient == nil {
		return nil, fmt.Errorf("nil pubsub client")
	}

	topic := pubsubClient.Topic(topicID)
	topicExists, err := topic.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("error when checking if topic %s exists: %w", topicID, err)
	}

	if !topicExists {
		return nil, fmt.Errorf("no topic with ID %s exists", topicID)
	}

	serviceAccountEmail, err := GetServiceAccountEmail()
	if err != nil {
		return nil, fmt.Errorf("error when getting service account email: %w", err)
	}

	// This is a PUSH type subscription, because Cloud Run is a *serverless*
	// platform and we cannot keep long lived pull subscriptions there. In a
	// future where this service is no longer run on a serverless platform, we
	// should switch to the higher throughput pull subscriptions.
	//
	// Authentication is via Google signed OpenID Connect tokens. For the Cloud
	// Run deployment, this authentication is automatic (done by Google). If we
	// move this deployment to another environment, we have to do our own
	// verification in the HTTP handler.
	return &pubsub.SubscriptionConfig{
		Topic: topic,
		PushConfig: pubsub.PushConfig{
			Endpoint: callbackURL,
			AuthenticationMethod: &pubsub.OIDCToken{
				Audience:            Aud,
				ServiceAccountEmail: serviceAccountEmail,
			},
		},
		AckDeadline:         ackDeadlineSeconds * time.Second,
		RetainAckedMessages: true,
		RetentionDuration:   time.Hour * hoursInAWeek,
		ExpirationPolicy:    time.Duration(0), // never expire
		RetryPolicy: &pubsub.RetryPolicy{
			MinimumBackoff: time.Second,
			MaximumBackoff: time.Second * maxBackoffSeconds,
		},
	}, nil
}

// SubscriptionIDs returns a map of topic IDs to subscription IDs
func SubscriptionIDs(topicIDs []string) map[string]string {
	output := map[string]string{}
	for _, topicID := range topicIDs {
		subscriptionID := topicID + "-default-subscription"
		output[topicID] = subscriptionID
	}
	return output
}

// ReverseSubscriptionIDs returns a (reversed) map of subscription IDs
// to topicIDs
func ReverseSubscriptionIDs(
	topicIDs []string,
	environment string,
	serviceName string,
	version string,
) map[string]string {
	output := map[string]string{}
	for _, topicID := range topicIDs {
		subscriptionID := topicID + "-default-subscription"
		output[subscriptionID] = topicID
	}
	return output
}

// NamespacePubsubIdentifier uses the service name, environment and version to
// create a "namespaced" pubsub identifier. This could be a topicID or
// subscriptionID.
func NamespacePubsubIdentifier(
	serviceName string,
	topicID string,
	environment string,
	version string,
) string {
	return fmt.Sprintf(
		"%s-%s-%s-%s",
		serviceName,
		topicID,
		environment,
		version,
	)
}

// PublishToPubsub sends the supplied payload to the indicated topic
func PublishToPubsub(
	ctx context.Context,
	pubsubClient *pubsub.Client,
	topicID string,
	environment string,
	serviceName string,
	version string,
	payload []byte,
) error {
	if pubsubClient == nil {
		return fmt.Errorf("nil pubsub client")
	}

	if payload == nil {
		return fmt.Errorf("nil payload")
	}

	t := pubsubClient.Topic(topicID)
	topicExists, err := t.Exists(ctx)
	if err != nil {
		return fmt.Errorf("error when checking if topic %s exists: %w", topicID, err)
	}
	if !topicExists {
		return fmt.Errorf("topic %s is not registered, can't publish to it", topicID)
	}

	result := t.Publish(ctx, &pubsub.Message{
		Data: payload,
		Attributes: map[string]string{
			"topicID": topicID,
		},
	})

	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	msgID, err := result.Get(ctx) // message id ignored for now
	if err != nil {
		return fmt.Errorf("unable to publish message: %w", err)
	}
	t.Stop() // clear the queue and stop the publishing goroutines
	log.Printf(
		"published to %s (%s), got back message ID %s", topicID, topicID, msgID)

	return nil
}

// GetServiceAccountEmail inspects the environment to get the project number
// and uses that to compose an email to use as a Google Cloud pub-sub email
func GetServiceAccountEmail() (string, error) {
	projectNumber, err := serverutils.GetEnvVar(GoogleProjectNumberEnvVarName)
	if err != nil {
		return "", fmt.Errorf(
			"no %s env var: %w", GoogleProjectNumberEnvVarName, err)
	}

	if projectNumber == "" {
		return "", fmt.Errorf("blank project number")
	}

	projectNumberInt, err := strconv.Atoi(projectNumber)
	if err != nil {
		return "", fmt.Errorf("can't convert project number to int: %w", err)
	}
	return fmt.Sprintf(
		"%d-compute@developer.gserviceaccount.com", projectNumberInt), nil
}
