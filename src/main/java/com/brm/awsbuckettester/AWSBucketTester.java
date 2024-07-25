package com.brm.awsbuckettester;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@SpringBootApplication
public class AWSBucketTester {

	private String bucketName;
	private String accessKeyId;
	private String secretAccessKey;

	public static void main(String[] args) {

		if (args.length < 3) {
			System.out.println("Usage: java AWSBucketTester <bucket name> <access key id> <secret access key>");
		} else {
			AWSBucketTester awsBucketTester = new AWSBucketTester();
			awsBucketTester.bucketName = args[0];
			awsBucketTester.accessKeyId = args[1];
			awsBucketTester.secretAccessKey = args[2];
			awsBucketTester.performBucketInteraction();
		}
	}

	public void performBucketInteraction() {

		S3Client s3Client = null;

		try {
			// Build the AWS credentials.
			AwsBasicCredentials credentials = AwsBasicCredentials.create(this.accessKeyId, this.secretAccessKey);

			// Create the S3 client.
			s3Client = S3Client.builder()
					.credentialsProvider(StaticCredentialsProvider.create(credentials))
					.region(Region.US_EAST_1)
					.build();

			// Create a test file to upload to the S3 bucket.
			Path testFilePath = createFileForTest();

			// Upload the file to the S3 bucket.
			uploadFile(s3Client, testFilePath);

			// Remove the test file.
			deleteTestFile(testFilePath);

			// Download the file.
			downloadFile(s3Client, testFilePath.getFileName().toString());

			// Remove the downloaded file.
			deleteTestFile(testFilePath);

		} catch (Exception e) {
			System.err.println(
					"An error occurred while interacting with the specified S3 bucket. Error: " + e.getMessage());
		} finally {

		}
	}

	private Path createFileForTest() {

		String testFileName = "test_" + System.currentTimeMillis() + ".txt";
		String testFileLocation = System.getProperty("user.dir") + File.separator + testFileName;
		Path testFilePath = Path.of(testFileLocation);

		try (BufferedWriter bw = Files.newBufferedWriter(testFilePath, StandardOpenOption.CREATE_NEW)) {
			bw.write("This test file was created at " + LocalDateTime.now() + ".");
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.err.println("The test file could not be created.  Error: " + ioe.getMessage());
		}

		return testFilePath;

	}

	/**
	 * This method uploads the test file at the given path to the S3 bucket.
	 * 
	 * @param s3Client     the client used to interact with the S3 bucket
	 * @param testFilePath the path to the test file to be uploaded to the S3 bucket
	 */
	private void uploadFile(S3Client s3Client, Path testFilePath) {

		try {
			// Build the request.
			PutObjectRequest putObjectRequest = PutObjectRequest.builder()
					.bucket(this.bucketName)
					.key(testFilePath.getFileName().toString())
					.build();

			// Make the request to upload the file to the S3 bucket.
			s3Client.putObject(putObjectRequest, testFilePath);
		} catch (Exception e) {
			System.err.println("The test file could not be uploaded to the bucket.  Error: " + e.getMessage());
		}
	}

	/**
	 * This method downloads the test file with the given name from the S3 bucket.
	 * 
	 * @param s3Client     the client used to interact with the S3 bucket
	 * @param testFileName the name of the test file to download from the S3 bucket
	 */
	private void downloadFile(S3Client s3Client, String testFileName) {

		FileOutputStream fileOutputStream = null;

		// Build the path to which the test file will be downloaded.
		Path downloadFilePath = Path.of(System.getProperty("user.dir") + File.separator + "download_" + testFileName);

		try {
			// Build the request.
			GetObjectRequest getObjectRequest = GetObjectRequest.builder()
					.bucket(this.bucketName)
					.key(testFileName)
					.build();

			// Make the request to download the test file from the S3 bucket.
			ResponseBytes<GetObjectResponse> dataResponse = s3Client.getObjectAsBytes(getObjectRequest);
			byte[] fileData = dataResponse.asByteArray();

			// Write the data to a file on the local file system.
			File downloadedFile = new File(downloadFilePath.toString());
			fileOutputStream = new FileOutputStream(downloadedFile);
			fileOutputStream.write(fileData);

		} catch (Exception e) {
			System.err.println("The test file could not be downloaded from the S3 bucket.  Error: " + e.getMessage());
		} finally {
			if (fileOutputStream != null) {
				try {
					fileOutputStream.close();
				} catch (IOException ioe) {
					System.err.println("The file output stream could not be closed.  Error: " + ioe.getMessage());
				}
			}
		}
	}

	/**
	 * This method deletes the test file at the given path.
	 * 
	 * @param testFilePath the path to the test file to be deleted
	 */
	private void deleteTestFile(Path testFilePath) {

		try {
			testFilePath.toFile().delete();
		} catch (Exception e) {
			System.err.println("The test file could not be deleted.  Error: " + e.getMessage());
		}
	}
}
