(async () => {
    const { google } = require('googleapis');
    const path = require('path');
    const AWS = require('aws-sdk');
    const { MongoClient } = require('mongodb');
    const { Builder, By, until } = require('selenium-webdriver');
    const dayjs = require('dayjs');
    const customParseFormat = require('dayjs/plugin/customParseFormat'); // Import customParseFormat plugin
    dayjs.extend(customParseFormat);
    const fetch = (await import('node-fetch')).default;
    const geocoder = require('@googlemaps/google-maps-services-js'); // Google Maps Geocoding client
    require('dotenv').config();

    // AWS S3 Configuration
    const S3_BUCKET = process.env.S3_BUCKET;
    const s3 = new AWS.S3({
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
        region: process.env.AWS_REGION,
    });

    // MongoDB Configuration
    const MONGO_URI = process.env.MONGO_URI;
    const MONGO_DB_NAME = process.env.MONGO_DB_NAME;
    let mongoClient;

    // List to track events that couldn't be uploaded successfully
    const failedEvents = [];

    // Load the service account credentials for Sheets API
    const auth = new google.auth.GoogleAuth({
        keyFile: path.join(__dirname, 'pinnit-upload-e9bfcdba3ad2.json'),
        scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
    });

    // Helper function to fetch data from Google Sheets
    async function getSheetData(spreadsheetId, range) {
        const authClient = await auth.getClient();
        const sheets = google.sheets({ version: 'v4', auth: authClient });

        console.log(`Fetching data from spreadsheetId: ${spreadsheetId}, range: ${range}`);

        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: spreadsheetId,
            range: range,
        });

        console.log('Response from Sheets API:', response.data);
        return response.data.values;
    }

    // Geocoding function to get latitude and longitude from location
    async function getCoordinates(location) {
        const client = new geocoder.Client({});
        try {
            const response = await client.geocode({
                params: {
                    address: location,
                    key: process.env.GOOGLE_API_KEY,
                },
            });

            if (response.data.results.length > 0) {
                const { lat, lng } = response.data.results[0].geometry.location;
                return { latitude: lat, longitude: lng };
            } else {
                return { latitude: null, longitude: null };
            }
        } catch (error) {
            console.error(`Error fetching coordinates for location: ${location}`, error);
            return { latitude: null, longitude: null };
        }
    }

    // MongoDB function to insert event data into dynamically named collection
    async function insertEventToMongoDB(eventData, eventDate) {
        try {
            const formattedDate = dayjs(eventDate).format('YYYY_MM_DD');
            const db = mongoClient.db(MONGO_DB_NAME);
            const collectionName = `Event_${formattedDate}`;
            const collection = db.collection(collectionName);
            const result = await collection.insertOne(eventData);
            console.log('Event inserted with ID:', result.insertedId);
        } catch (error) {
            console.error('Error inserting event data into MongoDB:', error);
            throw error; // Rethrow the error so it gets handled in the main loop
        }
    }

    // Function to insert into the Halloween collection
    async function insertEventToHalloweenCollection(eventData) {
        try {
            const db = mongoClient.db(MONGO_DB_NAME);
            const collection = db.collection('Halloween');
            const result = await collection.insertOne(eventData);
            console.log('Event inserted into Halloween collection with ID:', result.insertedId);
        } catch (error) {
            console.error('Error inserting event data into Halloween collection:', error);
            throw error;
        }
    }

    // Selenium function to scrape Instagram image URL by filtering for larger images
    async function scrapeInstagramImage(instagramUrl) {
        let driver = await new Builder().forBrowser('chrome').build();
        try {
            await driver.get(instagramUrl);

            const postImageElement = await driver.wait(
                until.elementLocated(By.css('article img[srcset], article img.FFVAD')),
                15000 // Set a timeout of 15 seconds
            );
            const imageUrl = await postImageElement.getAttribute('src');

            console.log(`Scraped Instagram Image URL: ${imageUrl}`);
            return imageUrl;
        } catch (error) {
            console.error(`Error scraping Instagram: ${error}`);
            throw error;
        } finally {
            await driver.quit();
        }
    }

    // Function to upload image to S3
    async function uploadImageToS3(imageUrl, fileName) {
        try {
            const imageResponse = await fetch(imageUrl);
            if (!imageResponse.ok) {
                throw new Error(`Failed to fetch image from URL: ${imageUrl}`);
            }
            const imageBuffer = Buffer.from(await imageResponse.arrayBuffer());

            console.log(`Fetched image size: ${imageBuffer.length} bytes`);

            const contentType = imageResponse.headers.get('content-type') || 'image/jpeg';

            const uploadParams = {
                Bucket: S3_BUCKET,
                Key: fileName,
                Body: imageBuffer,
                ACL: 'public-read',
                ContentType: contentType,
            };

            const data = await s3.upload(uploadParams).promise();
            console.log(`Image uploaded successfully to ${data.Location}`);
            return data.Location;
        } catch (error) {
            console.error('Error uploading image to S3:', error);
            throw error;
        }
    }

    // Helper function to convert time to 24-hour format using dayjs
    function convertTo24Hour(time) {
        if (!time) return null;

        const parsedTime = dayjs(time, ['h:mm A', 'h:mm a', 'H:mm'], true);
        if (parsedTime.isValid()) {
            return parsedTime.format('HH:mm');
        } else {
            console.error(`Invalid time format: ${time}`);
            return null;
        }
    }

    // Main logic to read from Google Sheets and upload to MongoDB
    const spreadsheetId = '1izC3vkNyVKWtVaYd6g8jX565bpde59ff_Fc9hulBHr4';
    const range = 'Sheet1!A2:J';

    try {
        mongoClient = await MongoClient.connect(MONGO_URI);
        console.log('Connected to MongoDB');

        const data = await getSheetData(spreadsheetId, range);
        for (let row of data) {
            const eventDate = row[0];
            const eventTitle = row[1];
            const hostOrganization = row[2];
            const startTime = row[3];
            const endTime = row[4];
            const location = row[5];
            const activityDescription = row[6];
            const registrationStatus = row[7];
            const referenceLink = row[8];
            const eventTags = row[9] ? row[9].split(',').map(tag => tag.trim()) : [];

            const startTime24 = convertTo24Hour(startTime);
            const endTime24 = convertTo24Hour(endTime);

            if (location && startTime24) {
                console.log(`Coordinates for ${location}...`);
                const coordinates = await getCoordinates(location);

                if (referenceLink) {
                    try {
                        const imageUrl = await scrapeInstagramImage(referenceLink);
                        const fileName = `${eventTitle.replace(/\s+/g, '_')}.jpg`;
                        const s3Url = await uploadImageToS3(imageUrl, fileName);

                        const eventData = {
                            event_date: eventDate,
                            event_title: eventTitle,
                            host_organization: hostOrganization,
                            start_time: startTime24,
                            end_time: endTime24,
                            location: location,
                            activity_description: activityDescription,
                            registration_status: registrationStatus,
                            reference_link: referenceLink,
                            image_url: s3Url,
                            latitude: coordinates.latitude,
                            longitude: coordinates.longitude,
                            tags: eventTags,
                            faculty: [],
                            degree_level: [],
                        };

                        // Insert event into MongoDB
                        await insertEventToMongoDB(eventData, eventDate);

                        // If the event contains the "halloween" tag, add it to the Halloween collection
                        if (eventTags.map(tag => tag.toLowerCase()).includes('halloween')) {
                            await insertEventToHalloweenCollection(eventData);
                        }

                    } catch (error) {
                        console.error(`Failed to process event: ${eventTitle}. Skipping.`);
                        failedEvents.push(eventTitle);
                    }
                } else {
                    console.log('Instagram URL is missing for event:', eventTitle);
                }
            } else {
                console.log(`Location or time is missing/invalid for event: ${eventTitle}`);
                failedEvents.push(eventTitle);
            }
        }
    } catch (err) {
        console.error('Error connecting to MongoDB or processing events:', err);
    } finally {
        if (mongoClient) {
            await mongoClient.close();
            console.log('MongoDB connection closed');
        }

        // Display the list of events that couldn't be uploaded
        if (failedEvents.length > 0) {
            console.log('The following events could not be uploaded:');
            failedEvents.forEach(eventTitle => console.log(`- ${eventTitle}`));
        } else {
            console.log('All events were uploaded successfully!');
        }
    }
})();
