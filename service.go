package azurestorage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
)

var (
	ctx = context.Background() // This example uses a never-expiring context.
)

// ================================================================================================================================================
// Azure Storage - BLOB Functions
// ================================================================================================================================================

func GetBlobService(accountName *string, accountKey *string, blobServiceURL *string) (azblob.ServiceURL, error) {

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

func GetBlobContainer(serviceURL azblob.ServiceURL, containerName *string) azblob.ContainerURL {
	// Now, you can use the serviceURL to perform various container and blob operations.

	// All HTTP operations allow you to specify a Go context.Context object to control cancellation/timeout.

	// This example shows several common operations just to get you started.

	// Create a URL that references a to-be-created container in your Azure Storage account.
	// This returns a ContainerURL object that wraps the container's URL and a request pipeline (inherited from serviceURL)
	return serviceURL.NewContainerURL(*containerName) // Container names require lowercase
}

func CreateBlobContainer(containerURL azblob.ContainerURL) error {
	// Create the container on the service (with no metadata and no public access)
	_, err := containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	if err != nil {
		return err
	}

	return nil
}

func DeleteBlobContainer(containerURL azblob.ContainerURL) error {
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

// ================================================================================================================================================
// Azure Storage - File Functions
// ================================================================================================================================================

func GetFileService(accountName *string, accountKey *string, fileServiceURL *string) (azfile.ServiceURL, error) {
	// Use your Storage account's name and key to create a credential object; this is used to access your account.
	credential, err := azfile.NewSharedKeyCredential(*accountName, *accountKey)
	if err != nil {
		return azfile.ServiceURL{}, err
	}

	// Create a request pipeline that is used to process HTTP(S) requests and responses. It requires
	// your account credentials. In more advanced scenarios, you can configure telemetry, retry policies,
	// logging, and other options. Also, you can configure multiple request pipelines for different scenarios.
	p := azfile.NewPipeline(credential, azfile.PipelineOptions{})

	// From the Azure portal, get your Storage account file service URL endpoint.
	// The URL typically looks like this:
	u, _ := url.Parse(fmt.Sprintf(*fileServiceURL, accountName))

	// Create an ServiceURL object that wraps the service URL and a request pipeline.
	return azfile.NewServiceURL(*u, p), nil
}

func GetFileShare(serviceURL azfile.ServiceURL, shareName *string) azfile.ShareURL {
	// This example shows several common operations just to get you started.

	// Create a URL that references a to-be-created share in your Azure Storage account.
	// This returns a ShareURL object that wraps the share's URL and a request pipeline (inherited from serviceURL)
	return serviceURL.NewShareURL(*shareName) // Share names require lowercase
}

func CreateFileShare(shareURL azfile.ShareURL) error {
	// Create the share on the service (with no metadata and default quota size)
	_, err := shareURL.Create(ctx, azfile.Metadata{}, 0)
	if err != nil && err.(azfile.StorageError) != nil && err.(azfile.StorageError).ServiceCode() != azfile.ServiceCodeShareAlreadyExists {
		return err
	}

	return nil
}

func UploadFile(shareURL azfile.ShareURL, fileName *string, data *string, fileContentType *string) (azfile.FileURL, error) {
	// Create a URL that references to root directory in your Azure Storage account's share.
	// This returns a DirectoryURL object that wraps the directory's URL and a request pipeline (inherited from shareURL)
	directoryURL := shareURL.NewRootDirectoryURL()

	// Create a URL that references a to-be-created file in your Azure Storage account's directory.
	// This returns a FileURL object that wraps the file's URL and a request pipeline (inherited from directoryURL)
	fileURL := directoryURL.NewFileURL(*fileName) // File names can be mixed case and is case insensitive

	// Create the file with string (plain text) content.
	length := int64(len(*data))
	_, err := fileURL.Create(ctx, length, azfile.FileHTTPHeaders{ContentType: *fileContentType}, azfile.Metadata{})
	if err != nil {
		return azfile.FileURL{}, err
	}

	_, err = fileURL.UploadRange(ctx, 0, strings.NewReader(*data), nil)
	if err != nil {
		return azfile.FileURL{}, err
	}

	return fileURL, nil
}

func DownloadFile(shareURL azfile.ShareURL, fileName *string) (string, error) {
	// Create a URL that references to root directory in your Azure Storage account's share.
	// This returns a DirectoryURL object that wraps the directory's URL and a request pipeline (inherited from shareURL)
	directoryURL := shareURL.NewRootDirectoryURL()

	// Create a URL that references a to-be-created file in your Azure Storage account's directory.
	// This returns a FileURL object that wraps the file's URL and a request pipeline (inherited from directoryURL)
	fileURL := directoryURL.NewFileURL(*fileName) // File names can be mixed case and is case insensitive

	// Download the file's contents and verify that it worked correctly.
	// User can specify 0 as offset and azfile.CountToEnd(-1) as count to indiciate downloading the entire file.
	get, err := fileURL.Download(ctx, 0, azfile.CountToEnd, false)
	if err != nil {
		return "", err
	}

	downloadedData := &bytes.Buffer{}
	retryReader := get.Body(azfile.RetryReaderOptions{})
	defer retryReader.Close() // The client must close the response body when finished with it

	downloadedData.ReadFrom(retryReader)
	return downloadedData.String(), nil
}

func GetListFile(shareURL azfile.ShareURL) ([][]azfile.FileItem, error) {
	var results [][]azfile.FileItem

	// Create a URL that references to root directory in your Azure Storage account's share.
	// This returns a DirectoryURL object that wraps the directory's URL and a request pipeline (inherited from shareURL)
	directoryURL := shareURL.NewRootDirectoryURL()

	// List the file(s) and directory(s) in our share's root directory; since a directory may hold millions of files and directories, this is done 1 segment at a time.
	for marker := (azfile.Marker{}); marker.NotDone(); { // The parentheses around azfile.Marker{} are required to avoid compiler error.
		// Get a result segment starting with the file indicated by the current Marker.
		listResponse, err := directoryURL.ListFilesAndDirectoriesSegment(ctx, marker, azfile.ListFilesAndDirectoriesOptions{})
		if err != nil {
			log.Fatal(err)
		}
		// IMPORTANT: ListFilesAndDirectoriesSegment returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listResponse.NextMarker

		// Process the files returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, fileEntry := range listResponse.FileItems {
			fmt.Println("File name: " + fileEntry.Name)
		}

		results = append(results, listResponse.FileItems)
	}

	return results, nil
}
