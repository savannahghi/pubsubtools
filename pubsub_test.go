package pubsubtools_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	pubsub "cloud.google.com/go/pubsub/v2"
	"github.com/rs/xid"
	base "github.com/savannahghi/pubsubtools"
	"github.com/savannahghi/serverutils"
	"github.com/segmentio/ksuid"
	"google.golang.org/api/idtoken"
)

func TestGetPubSubTopic(t *testing.T) {
	type args struct {
		m *base.PubSubPayload
	}

	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "nil payload",
			args: args{
				m: nil,
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "payload with no topic key",
			args: args{
				m: &base.PubSubPayload{
					Message: base.PubSubMessage{
						Attributes: map[string]string{
							"bad": "key",
						},
					},
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "payload withcorrect topic key",
			args: args{
				m: &base.PubSubPayload{
					Message: base.PubSubMessage{
						Attributes: map[string]string{
							"topicID": "some-topic",
						},
					},
				},
			},
			want:    "some-topic",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := base.GetPubSubTopic(tt.args.m)
			if (err != nil) != tt.wantErr {
				t.Errorf(
					"GetPubSubTopic() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)

				return
			}

			if got != tt.want {
				t.Errorf("GetPubSubTopic() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVerifyPubSubJWTAndDecodePayload(t *testing.T) {
	invalidHeaderReq := httptest.NewRequest(http.MethodPost, "/", nil)
	invalidHeaderReq.Header.Add("Authorization", "Bearer stuff")

	validHeaderReq := httptest.NewRequest(http.MethodPost, "/", nil)
	validHeaderReq.Header.Add("Authorization", "Bearer stuff")

	ctx := context.Background()

	goodIDTokenSource, err := idtoken.NewTokenSource(ctx, base.Aud)
	if err != nil {
		t.Errorf("can't initialize ID token source: %v", err)
		return
	}

	goodToken, err := goodIDTokenSource.Token()
	if err != nil {
		t.Errorf("error getting ID token: %v", err)
		return
	}

	goodTokenHeader := fmt.Sprintf(
		"%s %s", goodToken.TokenType, goodToken.AccessToken)
	testPayload := &base.PubSubPayload{
		Subscription: "test",
		Message: base.PubSubMessage{
			MessageID: ksuid.New().String(),
			Data:      []byte(ksuid.New().String()),
			Attributes: map[string]string{
				"topicID": ksuid.New().String(),
			},
		},
	}

	testPayloadBytes, err := json.Marshal(testPayload)
	if err != nil {
		t.Errorf("error marshalling test payload: %v", err)
		return
	}

	goodTokenReq := httptest.NewRequest(http.MethodPost, "/", bytes.NewBuffer(testPayloadBytes))
	goodTokenReq.Header.Add("Authorization", goodTokenHeader)

	badIssuerTokenSource, err := idtoken.NewTokenSource(ctx, "bad audience")
	if err != nil {
		t.Errorf("can't initialize ID token source: %v", err)
		return
	}

	badToken, err := badIssuerTokenSource.Token()
	if err != nil {
		t.Errorf("error getting ID token: %v", err)
		return
	}

	badTokenHeader := fmt.Sprintf("%s %s", badToken.TokenType, badToken.AccessToken)
	badTokenReq := httptest.NewRequest(http.MethodPost, "/", bytes.NewBuffer(testPayloadBytes))
	badTokenReq.Header.Add("Authorization", badTokenHeader)

	type args struct {
		w http.ResponseWriter
		r *http.Request
	}

	tests := []struct {
		name    string
		args    args
		want    *base.PubSubPayload
		wantErr bool
	}{
		{
			name: "no authorization header",
			args: args{
				w: &httptest.ResponseRecorder{},
				r: httptest.NewRequest(http.MethodPost, "/", nil),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "invalid authorization header",
			args: args{
				w: &httptest.ResponseRecorder{},
				r: invalidHeaderReq,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "valid authorization header",
			args: args{
				w: &httptest.ResponseRecorder{},
				r: goodTokenReq,
			},
			want:    testPayload,
			wantErr: false,
		},
		{
			name: "valid payload with wrong audience",
			args: args{
				w: &httptest.ResponseRecorder{},
				r: badTokenReq,
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := base.VerifyPubSubJWTAndDecodePayload(
				tt.args.w, tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Errorf(
					"VerifyPubSubJWTAndDecodePayload() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf(
					"VerifyPubSubJWTAndDecodePayload() = %v, want %v",
					got,
					tt.want,
				)
			}
		})
	}
}

func TestEnsureTopicsExist(t *testing.T) {
	ctx := context.Background()
	projectID := serverutils.MustGetEnvVar(base.GoogleCloudProjectIDEnvVarName)

	pubsubClient, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		t.Errorf("can't initialize pubsub client: %v", err)
		return
	}

	type args struct {
		ctx          context.Context
		pubsubClient *pubsub.Client
		topicIDs     []string
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "well known topic IDs that repeat from test case to test case",
			args: args{
				ctx:          ctx,
				pubsubClient: pubsubClient,
				topicIDs:     []string{"topic1", "topic2", "topic3"},
			},
			wantErr: false,
		},
		{
			name: "random topic IDs that are new from test case to test case",
			args: args{
				ctx:          ctx,
				pubsubClient: pubsubClient,
				topicIDs: []string{
					xid.New().String(),
					xid.New().String(),
				},
			},
			wantErr: false,
		},
		{
			name: "nil pubsub client",
			args: args{
				ctx:          ctx,
				pubsubClient: nil,
				topicIDs: []string{
					xid.New().String(),
					xid.New().String(),
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := base.EnsureTopicsExist(tt.args.ctx, tt.args.pubsubClient, tt.args.topicIDs); (err != nil) != tt.wantErr {
				t.Errorf("EnsureTopicsExist() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEnsureSubscriptionsExist(t *testing.T) {
	ctx := context.Background()
	projectID := serverutils.MustGetEnvVar(base.GoogleCloudProjectIDEnvVarName)

	pubsubClient, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		t.Errorf("can't initialize pubsub client: %v", err)
		return
	}

	environment := "CI"
	serviceName := "test"
	version := "v1"
	validTopic := "test.ci"
	validTopics := []string{
		base.NamespacePubsubIdentifier(
			serviceName,
			validTopic,
			environment,
			version,
		),
	}

	err = base.EnsureTopicsExist(ctx, pubsubClient, validTopics)
	if err != nil {
		t.Errorf("can't create topics: %v", err)
		return
	}

	type args struct {
		ctx                  context.Context
		pubsubClient         *pubsub.Client
		topicSubscriptionMap map[string]string
		callbackURL          string
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "topic that does not exist - expect error",
			args: args{
				ctx:          ctx,
				pubsubClient: pubsubClient,
				topicSubscriptionMap: map[string]string{
					"fake-topic-1": "subscription-to-fake-topic",
				},
				callbackURL: "https://dummy.healthcloud.co.ke/pubsub/",
			},
			wantErr: true,
		},
		{
			name: "nil pubsub client",
			args: args{
				ctx:          ctx,
				pubsubClient: nil,
				topicSubscriptionMap: map[string]string{
					"fake-topic-1": "subscription-to-fake-topic",
				},
				callbackURL: "https://dummy.healthcloud.co.ke/pubsub/",
			},
			wantErr: true,
		},
		{
			name: "valid topic and subscription",
			args: args{
				ctx:          ctx,
				pubsubClient: pubsubClient,
				topicSubscriptionMap: map[string]string{
					validTopic: "valid-sub",
				},
				callbackURL: "https://dummy.healthcloud.co.ke/pubsub/",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := base.EnsureSubscriptionsExist(
				tt.args.ctx,
				tt.args.pubsubClient,
				tt.args.topicSubscriptionMap,
				tt.args.callbackURL,
			); (err != nil) != tt.wantErr {
				t.Errorf(
					"EnsureSubscriptionsExist() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)
			}
		})
	}
}

func TestGetPushSubscriptionConfig(t *testing.T) {
	ctx := context.Background()
	projectID := serverutils.MustGetEnvVar(base.GoogleCloudProjectIDEnvVarName)

	pubsubClient, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		t.Errorf("can't initialize pubsub client: %v", err)
		return
	}

	validTopic := "test.ci"

	err = base.EnsureTopicsExist(ctx, pubsubClient, []string{validTopic})
	if err != nil {
		t.Errorf("can't create topic %s: %v", validTopic, err)
		return
	}

	type args struct {
		ctx          context.Context
		pubsubClient *pubsub.Client
		topicID      string
		callbackURL  string
	}

	tests := []struct {
		name    string
		args    args
		wantNil bool
		wantErr bool
	}{
		{
			name: "nil pubsub client",
			args: args{
				ctx:          context.Background(),
				pubsubClient: nil,
				topicID:      validTopic,
				callbackURL:  "https://dummy.healthcloud.co.ke/pubsub/",
			},
			wantNil: true,
			wantErr: true,
		},
		{
			name: "non-nil pubsub client, topic that does not exist",
			args: args{
				ctx:          context.Background(),
				pubsubClient: pubsubClient,
				topicID:      xid.New().String(),
				callbackURL:  "https://dummy.healthcloud.co.ke/pubsub/",
			},
			wantNil: true,
			wantErr: true,
		},
		{
			name: "non-nil pubsub client, topic that exists",
			args: args{
				ctx:          context.Background(),
				pubsubClient: pubsubClient,
				topicID:      validTopic,
				callbackURL:  "https://dummy.healthcloud.co.ke/pubsub/",
			},
			wantNil: false,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := base.GetPushSubscriptionConfig(
				tt.args.ctx,
				tt.args.pubsubClient,
				tt.args.topicID,
				tt.args.callbackURL,
			)
			if (err != nil) != tt.wantErr {
				t.Errorf(
					"GetPushSubscriptionConfig() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)

				return
			}

			if !tt.wantNil && got == nil {
				t.Errorf("got nil pubsub push config, expected non nil")
				return
			}
		})
	}
}

func TestSubscriptionIDs(t *testing.T) {
	type args struct {
		topicIDs []string
	}

	tests := []struct {
		name string
		args args
		want map[string]string
	}{
		{
			name: "good case",
			args: args{
				topicIDs: []string{"topic1", "topic2"},
			},
			want: map[string]string{
				"topic1": "topic1-default-subscription",
				"topic2": "topic2-default-subscription",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := base.SubscriptionIDs(
				tt.args.topicIDs,
			); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SubscriptionIDs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReverseSubscriptionIDs(t *testing.T) {
	type args struct {
		topicIDs []string
	}

	tests := []struct {
		name string
		args args
		want map[string]string
	}{
		{
			name: "good case",
			args: args{
				topicIDs: []string{"topic1", "topic2"},
			},
			want: map[string]string{
				"topic1-default-subscription": "topic1",
				"topic2-default-subscription": "topic2",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := base.ReverseSubscriptionIDs(
				tt.args.topicIDs,
			); !reflect.DeepEqual(got, tt.want) {
				t.Errorf(
					"ReverseSubscriptionIDs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNamespacePubsubIdentifier(t *testing.T) {
	type args struct {
		serviceName string
		topicID     string
		environment string
		version     string
	}

	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "valid case",
			args: args{
				serviceName: "test",
				topicID:     "test",
				environment: "ci",
				version:     "v1",
			},
			want: "test-test-ci-v1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := base.NamespacePubsubIdentifier(
				tt.args.serviceName,
				tt.args.topicID,
				tt.args.environment,
				tt.args.version,
			); got != tt.want {
				t.Errorf(
					"NamespacePubsubIdentifier() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPublishToPubsub(t *testing.T) {
	ctx := context.Background()
	projectID := serverutils.MustGetEnvVar(base.GoogleCloudProjectIDEnvVarName)

	pubsubClient, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		t.Errorf("can't initialize pubsub client: %v", err)
		return
	}

	environment := "CI"
	serviceName := "test"
	version := "v1"

	validTopic := "test.ci"
	validTopics := []string{
		base.NamespacePubsubIdentifier(
			serviceName,
			validTopic,
			environment,
			version,
		),
	}

	err = base.EnsureTopicsExist(ctx, pubsubClient, validTopics)
	if err != nil {
		t.Errorf("can't create topics: %v", err)
		return
	}

	type args struct {
		ctx          context.Context
		pubsubClient *pubsub.Client
		topicName    string
		payload      []byte
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "topic that does not exist",
			args: args{
				ctx:          ctx,
				pubsubClient: pubsubClient,
				topicName:    ksuid.New().String(),
				payload:      []byte("some payload"),
			},
			wantErr: true,
		},
		{
			name: "topic that exists",
			args: args{
				ctx:          ctx,
				pubsubClient: pubsubClient,
				topicName:    validTopic,
				payload:      []byte("some payload"),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := base.PublishToPubsub(
				tt.args.ctx,
				tt.args.pubsubClient,
				tt.args.topicName,
				tt.args.payload,
			); (err != nil) != tt.wantErr {
				t.Errorf(
					"PublishToPubsub() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)
			}
		})
	}
}

func TestGetServiceAccountEmail(t *testing.T) {
	projectNumber := serverutils.MustGetEnvVar(base.GoogleProjectNumberEnvVarName)
	expectedServiceAccountEmail := fmt.Sprintf(
		"%s-compute@developer.gserviceaccount.com", projectNumber)
	tests := []struct {
		name    string
		want    string
		wantErr bool
	}{
		{
			name:    "valid case, correct env set",
			want:    expectedServiceAccountEmail,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := base.GetServiceAccountEmail()
			if (err != nil) != tt.wantErr {
				t.Errorf(
					"GetServiceAccountEmail() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)

				return
			}

			if got != tt.want {
				t.Errorf(
					"GetServiceAccountEmail() = %v, want %v",
					got,
					tt.want,
				)
			}
		})
	}
}
