import mailgun from "mailgun-js";
import fs from 'fs';
import https from 'https';
import {Storage} from '@google-cloud/storage';
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
    DynamoDBDocumentClient,
    PutCommand,

} from "@aws-sdk/lib-dynamodb";

export const handler = async (event, context) => {
    try {
        console.log("Start of handler function");
        const message = event.Records[0].Sns.Message;
        var messageObject = JSON.parse(message);
        console.log("EVENT: \n" + JSON.stringify(messageObject.assignmentId, null, 2));
        await downloadFile(messageObject);
        var url =await uploadFile( messageObject);
        var status = await sendMail(messageObject, "SUCCESS");
        console.log("Status of the mail: " + status)
        await putItem(messageObject,status, "SUCCESS");
        console.log("End of handler function");
        return context.logStreamName;
    } catch (error) {
        console.error("Error in handler function: " + error.message);
    }
};


async function uploadFile(messageObject) {
    const filePath = `/tmp/${messageObject.submissionId}.zip`;
    const destinationPath = `/uploads/${messageObject.emailId}/${messageObject.assignmentId}/${messageObject.submissionId}.zip`
    const base64Key = process.env.GCP_SERVICE_ACCOUNT_KEY;
    const keyBuffer = Buffer.from(base64Key, 'base64');
    const keyContents = keyBuffer.toString('utf-8');
    const projectId = process.env.GCP_PROJECT_ID;

    const jsonObject = JSON.parse(keyContents);

    const storage = new Storage({
        projectId: projectId,
        credentials: {
            client_email: jsonObject.client_email,
            private_key: jsonObject.private_key
        }
    });

    const bucketName = process.env.GCP_BUCKET_NAME;
    console.log("Bucket Name: " + bucketName);
    try {
        await storage.bucket(bucketName).upload(filePath, {
            destination: destinationPath,
        });

        console.log(`File ${filePath} uploaded to ${bucketName}/${destinationPath}`);
        return destinationPath;
    } catch (error) {
        console.error('Error uploading file:', error);
        messageObject.reason = 'Error during upload';
        const status = await sendMail(messageObject, "FAILED");
        await putItem(messageObject,status,"UPLOAD_FAILED");
        throw error;
    }
}

async function putItem(messageObject,status,downLoadStatus) {

    const client = new DynamoDBClient({});
    const dynamo = DynamoDBDocumentClient.from(client);

    const tableName = "assignment-submissions";
    try{
        console.log("Updating dynamo db");
        await dynamo.send(
            new PutCommand({
                TableName: tableName,
                Item: {
                    submission_id: messageObject.submissionId,
                    assignment_id: messageObject.assignmentId,
                    submission_url: messageObject.submissionUrl,
                    email_id: messageObject.emailId,
                    timestamp: Date.now(),
                    mail_status: status,
                    download_status: downLoadStatus,
                },
            })
        );
        console.log("Updated dynamo db");
    }catch (error){
        console.error("Error while updating dynamo db: " + error.message);
    }
}

async function downloadFile(messageObject) {
    const destinationPath = `/tmp/${messageObject.submissionId}.zip`;
    const url = messageObject.submissionUrl;
    await new Promise((resolve, reject) => {
        const file = fs.createWriteStream(destinationPath);

        https.get(url, async response => {
            if (response.statusCode === 200) {
                response.pipe(file);

                file.on('finish', () => {
                    file.close(async () => {
                        console.log(`Download completed: ${destinationPath}`);
                        const stats = fs.statSync(destinationPath);
                        const fileSizeInBytes = stats.size;
                        if (fileSizeInBytes === 0) {
                            console.error("File size is 0");
                            const status = await sendMail(messageObject, "EMPTY_FILE");
                            await putItem(messageObject, status, "EMPTY_FILE")
                            reject(new Error("File size is 0"));
                        }
                        resolve(); // Resolve the promise to signal completion
                    });
                });
            } else if (response.statusCode === 302) {
                console.log("Redirecting to: " + response.headers.location);
                https.get(response.headers.location, async response2 => {
                    if (response2.statusCode === 200) {
                        response2.pipe(file);
                        file.on('finish', () => {
                            file.close(async () => {
                                console.log(`Download completed: ${destinationPath}`);
                                const stats = fs.statSync(destinationPath);
                                const fileSizeInBytes = stats.size;
                                if (fileSizeInBytes === 0) {
                                    console.error("File size is 0");
                                    const status = await sendMail(messageObject, "EMPTY_FILE");
                                    await putItem(messageObject, status, "EMPTY_FILE")
                                    reject(new Error("File size is 0"));
                                }
                                resolve(); // Resolve the promise to signal completion
                            });
                        });
                    } else {
                        console.error(`Failed to download file. Status code: ${response2.statusCode}`);
                        messageObject.reason = 'Error during download';
                        const status = await sendMail(messageObject, "FAILED");
                        await putItem(messageObject, status, "FAILED")
                        reject(new Error(`Failed to download file. Status code: ${response2.statusCode}`));
                    }
                }).on('error', async error => {
                    console.error(`Error during download: ${error.message}`);
                    messageObject.reason = 'Error during download';
                    const status = await sendMail(messageObject, "FAILED");
                    await putItem(messageObject,status,"FAILED")
                    reject(error);
                });
            } else {
                console.error(`Failed to download file. Status code: ${response.statusCode}`);
                messageObject.reason = 'Error during download';
                const status = await sendMail(messageObject, "FAILED");
                await putItem(messageObject,status,"FAILED")
                reject(new Error(`Failed to download file. Status code: ${response.statusCode}`));
            }
        }).on('error', async error => {
            console.error(`Error during download: ${error.message}`);
            messageObject.reason = 'Error during download';
            const status = await sendMail(messageObject, "FAILED");
            await putItem(messageObject, status, "FAILED")
            reject(error);
        });
    });
}

async function sendMail(messageObject,status) {

    const apiKey = "38fbb8eabd59a2c797f768c5a110634c-30b58138-8ddd986d";
    const domain = "demo.webapplicationforcloud.me";
    const mailgunInstance = mailgun({ apiKey, domain });
    let data = {};
    if (status == "SUCCESS") {
        data = {
            from: "no-reply@demo.webapplicationforcloud.me",
            to: messageObject.emailId,
            subject: "File Submission suceessfully made" ,
            html: `
        <p>Hello ,</p>
        <p>Your assignment file with email_address "${messageObject.emailId}" has been submitted successfully.</p>
        <p>Assignment_ID:"${messageObject.assignmentId}"<p>
        <p>Submission_ID:"${messageObject.submissionId}"<p>
        <p>Location : "/uploads/${messageObject.emailId}/${messageObject.assignmentId}/${messageObject.submissionId}.zip"<p>
        <p>Thank you for the submission.</p>
        `,
        };
    } else if(status === "EMPTY_FILE" ||status === "FAILED") {
        data = {
            from: "no-reply@demo.webapplicationforcloud.me",
            to: messageObject.emailId,
            subject: "Submission did not take place " ,
            html: `
        <p>Hello "${messageObject.emailId}",</p>
        <p>Your assignment submission for the assignment is failed, Due to Invalid Submission URL".</p>
        <p>Assignment_ID:"${messageObject.assignmentId}"<p>
        <p>Submission_ID:"${messageObject.submissionId}"<p>        
       
        `,
        };
    }

    mailgunInstance.messages().send(data, (error, body) => {
        if (error) {
            console.error(
                `Failed to send email to ${messageObject.emailId} for assignment ${messageObject.assignmentName}`
            );
            console.error(error);
            throw error;
        } else {
            console.log(
                `Email sent to ${messageObject.emailId} for assignment ${messageObject.assignmentName}`
            );
        }
    });



}
