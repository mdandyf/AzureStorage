package azurestorage

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

var (
	ctx = context.Background() // This example uses a never-expiring context.
)

func GetService(accountName *string, accountKey *string, blobServiceURL *string) (azblob.ServiceURL, error) {

	// Use your Storage account's name and key to create a credential object; this is used to access your account.
	credential, err := azblob.NewSharedKeyCredential(*accountName, *accountKey)
	if err != nil {
		return azblob.ServiceURL{}, err
	}

	// Create a request pipeline that is used to process HTTP(S) requests and responses. It requires
	// your account credentials. In more advanced scenarios, you can configure telemetry, retry policies,
	// logging, and other options. Also, you can configure multiple request pipelines for different scenarios.
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// From the Azure portal, get your Storage account blob service URL endpoint.
	// The URL typically looks like this:
	u, _ := url.Parse(fmt.Sprintf(*blobServiceURL, *accountName))

	// Create an ServiceURL object that wraps the service URL and a request pipeline.
	return azblob.NewServiceURL(*u, p), nil
}

func GetContainer(serviceURL azblob.ServiceURL, containerName *string) azblob.ContainerURL {
	// Now, you can use the serviceURL to perform various container and blob operations.

	// All HTTP operations allow you to specify a Go context.Context object to control cancellation/timeout.

	// This example shows several common operations just to get you started.

	// Create a URL that references a to-be-created container in your Azure Storage account.
	// This returns a ContainerURL object that wraps the container's URL and a request pipeline (inherited from serviceURL)
	return serviceURL.NewContainerURL(*containerName) // Container names require lowercase
}

func CreateContainer(containerURL azblob.ContainerURL) error {
	// Create the container on the service (with no metadata and no public access)
	_, err := containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	if err != nil {
		return err
	}

	return nil
}

func DeleteContainer(containerURL azblob.ContainerURL) error {
	// Delete the container we created earlier.
	_, err := containerURL.Delete(ctx, azblob.ContainerAccessConditions{})
	if err != nil {
		return err
	}

	return nil
}

func UploadBlob(containerURL azblob.ContainerURL, blobName *string, blobType *string, data io.ReadSeeker) (azblob.BlockBlobURL, error) {
	// Create a URL that references a to-be-created blob in your Azure Storage account's container.
	// This returns a BlockBlobURL object that wraps the blob's URL and a request pipeline (inherited from containerURL)
	blobURL := containerURL.NewBlockBlobURL(*blobName) // Blob names can be mixed case

	// Upload the blob
	_, err := blobURL.Upload(ctx, data, azblob.BlobHTTPHeaders{ContentType: *blobType}, azblob.Metadata{}, azblob.BlobAccessConditions{}, azblob.DefaultAccessTier, nil, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return azblob.BlockBlobURL{}, err
	}

	return blobURL, nil
}

func DownloadBlob(containerURL azblob.ContainerURL, blobName *string) (*azblob.DownloadResponse, error) {
	// Create a URL that references a to-be-created blob in your Azure Storage account's container.
	// This returns a BlockBlobURL object that wraps the blob's URL and a request pipeline (inherited from containerURL)
	blobURL := containerURL.NewBlockBlobURL(*blobName) // Blob names can be mixed case

	// Download the blob's contents and verify that it worked correctly
	return blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
}

func DeleteBlob(containerURL azblob.ContainerURL, blobName *string) error {
	// Create a URL that references a to-be-created blob in your Azure Storage account's container.
	// This returns a BlockBlobURL object that wraps the blob's URL and a request pipeline (inherited from containerURL)
	blobURL := containerURL.NewBlockBlobURL(*blobName) // Blob names can be mixed case

	// Delete the blob
	_, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	if err != nil {
		return err
	}

	return nil
}

func GetListBlob(containerURL azblob.ContainerURL) ([][]azblob.BlobItemInternal, error) {
	var results [][]azblob.BlobItemInternal

	// List the blob(s) in our container; since a container may hold millions of blobs, this is done 1 segment at a time.
	for marker := (azblob.Marker{}); marker.NotDone(); { // The parens around Marker{} are required to avoid compiler error.
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})
		if err != nil {
			return nil, err
		}
		// IMPORTANT: ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			fmt.Print("Blob name: " + blobInfo.Name + "\n")
		}

		results = append(results, listBlob.Segment.BlobItems)
	}

	return results, nil
}
