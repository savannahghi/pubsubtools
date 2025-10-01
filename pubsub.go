package pubsubtools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	pubsub "cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/savannahghi/serverutils"
	"google.golang.org/api/idtoken"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
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
	minBackoffSeconds  = 1
	secondsInAWeek     = 604800
)

// PubSubMessage is a pub-sub message payload.
//
// See https://cloud.google.com/pubsub/docs/push for more context.
//
// The message that is POSTed looks like the example below:
//
//	{
//	    "message": {
//	        "attributes": {
//	            "key": "value"
//	        },
//	        "data": "SGVsbG8gQ2xvdWQgUHViL1N1YiEgSGVyZSBpcyBteSBtZXNzYWdlIQ==",
//	        "messageId": "136969346945"
//	    },
//	   "subscription": "projects/myproject/subscriptions/mysubscription"
//	}
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
	_ http.ResponseWriter,
	r *http.Request,
) (*PubSubPayload, error) {
	authHeader := r.Header.Get(authHeaderName)
	if authHeader == "" || len(strings.Split(authHeader, " ")) != 2 {
		return nil, fmt.Errorf("missing Authorization Header")
	}

	token := strings.Split(authHeader, " ")[1]

	payload, err := idtoken.Validate(r.Context(), token, Aud)
	if err != nil {
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	if payload.Issuer != googleIss && payload.Issuer != googleAccountsIss {
		return nil, fmt.Errorf("%s is not a valid issuer", payload.Issuer)
	}

	// decode the message
	var m PubSubPayload

	body, err := io.ReadAll(r.Body)
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

	// ensure topics in the given list exist or create missing ones
	for _, topicID := range topicIDs {
		topicName := fullyQualifiedName(pubsubClient.Project(), "topics", topicID)

		_, err := pubsubClient.TopicAdminClient.GetTopic(
			ctx,
			&pubsubpb.GetTopicRequest{
				Topic: topicName,
			},
		)
		if err != nil {
			if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
				_, err := pubsubClient.TopicAdminClient.CreateTopic(
					ctx,
					&pubsubpb.Topic{
						Name: topicName,
					},
				)
				if err != nil {
					if st, ok := status.FromError(err); ok && st.Code() != codes.AlreadyExists {
						return fmt.Errorf("could not create topic %s: %w", topicID, err)
					}
				}
			} else {
				return fmt.Errorf("could not check if topic %s exists: %w", topicID, err)
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
		topicName := fullyQualifiedName(pubsubClient.Project(), "topics", topicID)
		subName := fullyQualifiedName(pubsubClient.Project(), "subscriptions", subscriptionID)

		_, err := pubsubClient.TopicAdminClient.GetTopic(
			ctx,
			&pubsubpb.GetTopicRequest{
				Topic: topicName,
			},
		)
		if err != nil {
			if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
				return fmt.Errorf("no topic with ID %s exists", topicID)
			}

			return fmt.Errorf("could not check if topic %s exists: %w", topicID, err)
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

		subscriptionConfig.Name = subName

		_, err = pubsubClient.SubscriptionAdminClient.CreateSubscription(ctx, subscriptionConfig)
		if status.Code(err) == codes.AlreadyExists {
			continue
		} else if err != nil {
			log.Printf("Detailed error:\n%#v\n", err)
			return fmt.Errorf("can't create subscription %s: %w", subName, err)
		}

		log.Printf("created subscription %#v with config %#v", subName, subscriptionConfig)
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
) (*pubsubpb.Subscription, error) {
	if pubsubClient == nil {
		return nil, fmt.Errorf("nil pubsub client")
	}

	topicName := fullyQualifiedName(pubsubClient.Project(), "topics", topicID)

	_, err := pubsubClient.TopicAdminClient.GetTopic(
		ctx,
		&pubsubpb.GetTopicRequest{
			Topic: topicName,
		},
	)
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			return nil, fmt.Errorf("no topic with ID %s exists", topicID)
		}

		return nil, fmt.Errorf("could not check if topic %s exists: %w", topicID, err)
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
	subConfig := &pubsubpb.Subscription{
		Topic: topicName,
		PushConfig: &pubsubpb.PushConfig{
			PushEndpoint: callbackURL,
			AuthenticationMethod: &pubsubpb.PushConfig_OidcToken_{
				OidcToken: &pubsubpb.PushConfig_OidcToken{
					ServiceAccountEmail: serviceAccountEmail,
					Audience:            Aud,
				},
			},
		},
		AckDeadlineSeconds:  ackDeadlineSeconds,
		RetainAckedMessages: true,
		MessageRetentionDuration: &durationpb.Duration{
			Seconds: secondsInAWeek,
		},
		RetryPolicy: &pubsubpb.RetryPolicy{
			MinimumBackoff: &durationpb.Duration{
				Seconds: minBackoffSeconds,
			},
			MaximumBackoff: &durationpb.Duration{
				Seconds: maxBackoffSeconds,
			},
		},
	}

	return subConfig, nil
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
	payload []byte,
) error {
	if pubsubClient == nil {
		return fmt.Errorf("nil pubsub client")
	}

	if payload == nil {
		return fmt.Errorf("nil payload")
	}

	topicName := fullyQualifiedName(pubsubClient.Project(), "topics", topicID)

	_, err := pubsubClient.TopicAdminClient.GetTopic(
		ctx,
		&pubsubpb.GetTopicRequest{
			Topic: topicName,
		},
	)
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			return fmt.Errorf("no topic with ID %s exists, can't publish to it", topicID)
		}

		return fmt.Errorf("could not check if topic %s exists: %w", topicID, err)
	}

	pub := pubsubClient.Publisher(topicName)

	result := pub.Publish(ctx, &pubsub.Message{
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

	pub.Stop() // clear the queue and stop the publishing goroutines
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

func fullyQualifiedName(projectID, resourceType, resource string) string {
	return fmt.Sprintf("projects/%s/%s/%s", projectID, resourceType, resource)
}
