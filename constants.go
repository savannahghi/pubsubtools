package pubsubtools

import "time"

/* #nosec */
const (

	// GoogleProjectNumberEnvVarName is a numeric project number that
	GoogleProjectNumberEnvVarName = "GOOGLE_PROJECT_NUMBER"

	// GoogleCloudProjectIDEnvVarName is used to determine the ID of the GCP project e.g for setting up StackDriver client
	GoogleCloudProjectIDEnvVarName = "GOOGLE_CLOUD_PROJECT"
)

var (

	// TimeLocation default timezone
	TimeLocation, _ = time.LoadLocation("Africa/Nairobi")
)
