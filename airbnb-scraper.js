const puppeteer = require('puppeteer');
const { parse } = require('json2csv');
const winston = require('winston');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const LOCATION = "us";
const MAX_RETRIES = 2;
const MAX_SEARCH_DATA_THREADS = 2;
const MAX_REVIEW_DATA_THREADS = 5;
const PAGES = 4;

const keywordList = ["Myrtle Beach, South Carolina, United States"];

const { api_key: API_KEY } = JSON.parse(fs.readFileSync('config.json', 'utf8'));

function getScrapeOpsUrl(url, location = "us") {
    const params = new URLSearchParams({
        api_key: API_KEY,
        url,
        country: LOCATION,
        wait: 5000,
    });
    return `https://proxy.scrapeops.io/v1/?${params.toString()}`;
}

const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.printf(({ timestamp, level, message }) => {
            return `${timestamp} [${level.toUpperCase()}]: ${message}`;
        })
    ),
    transports: [
        new winston.transports.Console(),
        new winston.transports.File({ filename: 'scraper.log' })
    ]
});

class SearchData {
    constructor(name = "", description = "", dates = "", price = "", url = "") {
        this.name = name.trim() || "No name";
        this.description = description.trim() || "No description";
        this.dates = dates.trim() || "No dates";
        this.price = price.trim() || "No price";
        this.url = url.trim() || "No url";
    }
}

class ReviewData {
    constructor(name = "", stars = 0, review = "") {
        this.name = name;
        this.stars = stars;
        this.review = review;

        this.checkStringFields();
    }

    checkStringFields() {
        const fields = ['name', 'stars', 'review'];

        fields.forEach(field => {
            if (typeof this[field] === 'string') {
                if (this[field] === "") {
                    this[field] = `No ${field}`;
                } else {
                    this[field] = this[field].trim();
                }
            }
        });
    }
}

class DataPipeline {
    constructor(csvFilename, storageQueueLimit = 50) {
        this.namesSeen = new Set();
        this.storageQueue = [];
        this.storageQueueLimit = storageQueueLimit;
        this.csvFilename = csvFilename;
    }

    async saveToCsv() {
        const filePath = path.resolve(this.csvFilename);
        const dataToSave = this.storageQueue.splice(0, this.storageQueue.length);
        if (dataToSave.length === 0) return;

        const csvData = parse(dataToSave, { header: !fs.existsSync(filePath) });
        fs.appendFileSync(filePath, csvData + '\n', 'utf8');
    }

    isDuplicate(name) {
        if (this.namesSeen.has(name)) {
            logger.warn(`Duplicate item found: ${name}. Item dropped.`);
            return true;
        }
        this.namesSeen.add(name);
        return false;
    }

    async addData(data) {
        if (!this.isDuplicate(data.name)) {
            this.storageQueue.push(data);
            if (this.storageQueue.length >= this.storageQueueLimit) {
                await this.saveToCsv();
            }
        }
    }

    async closePipeline() {
        if (this.storageQueue.length > 0) await this.saveToCsv();
    }
}

async function findPaginationUrls(keyword, pages = PAGES) {
    const browser = await puppeteer.launch({ headless: true });
    const page = await browser.newPage();
    const formattedKeyword = keyword.replace(/, /g, "--").replace(/ /g, "-");
    const url = `https://www.airbnb.com/s/${formattedKeyword}/homes`;
    const links = [url];

    try {
        await page.goto(getScrapeOpsUrl(url), { waitUntil: 'networkidle2' });
        const paginationLinks = await page.$$eval("nav[aria-label='Search results pagination'] a", anchors => anchors.map(a => a.href));
        links.push(...paginationLinks.slice(0, pages - 1));
    } catch (error) {
        logger.error(`Error fetching pagination URLs: ${error}`);
    } finally {
        await browser.close();
    }

    logger.info("Pagination Links Collected")
    return links;
}

async function scrapeSearchResults(browser, url, dataPipeline, retries = MAX_RETRIES) {
    const page = await browser.newPage();
    let tries = 0;
    let success = false;

    while (tries <= retries && !success) {
        try {
            await page.goto(getScrapeOpsUrl(url), { waitUntil: 'networkidle2' });
            const dataItems = await page.evaluate(() => {
                const cards = document.querySelectorAll("div[data-testid='card-container']");
                return Array.from(cards).map(card => {
                    const name = card.querySelector("div[data-testid='listing-card-subtitle']")?.textContent;
                    const description = card.querySelector("div[data-testid='listing-card-title']")?.textContent;
                    const dates = card.querySelector("div[data-testid='listing-card-subtitle']:nth-of-type(4) span span")?.textContent;
                    const price = card.querySelector("span div span")?.textContent;
                    const href = card.querySelector("a")?.href;
                    return { name, description, dates, price, url: href};
                });
            });
            for (const item of dataItems) {
                await dataPipeline.addData(new SearchData(item.name, item.description, item.dates, item.price, item.url));
            }

            logger.info(`Successfully scraped data from ${url}`);
            success = true;
        } catch (error) {
            logger.error(`Error scraping data from ${url}: ${error}`);
            tries += 1;
            await page.reload({ waitUntil: 'networkidle2' });
        }
    }

    await page.close();
    if (!success) throw new Error(`Failed to scrape ${url} after ${retries} retries`);
}

async function processListing(browser, row, retries = 3) {
    const url = row.url;
    let tries = 0;
    let success = false;

    const page = await browser.newPage();

    while (tries < retries && !success) {
        try {
            logger.info(`Attempting to navigate to ${url} (Attempt ${tries + 1})`);
            await page.goto(getScrapeOpsUrl(url), { waitUntil: 'networkidle2' });

            const reviewCards = await page.$$("div[role='listitem']");
            const reviewPipeline = new DataPipeline(`${row.name.replace(/\s+/g, '-').replace(/\//g, '-')}.csv`);

            for (const reviewCard of reviewCards) {
                const nameElement = await reviewCard.$("h3");
                const starsElements = await reviewCard.$$("svg");
                const spans = await reviewCard.$$("span");
                const reviewElement = spans[spans.length - 1];

                const name = nameElement ? await page.evaluate(el => el.textContent, nameElement) : 'Unknown';
                const stars = await Promise.all(starsElements.map(async (star) => {
                    return await page.evaluate(el => getComputedStyle(el).fill, star);
                }));

                const filledStars = stars.filter(color => color === 'rgb(34, 34, 34)').length;
                const review = reviewElement ? await page.evaluate(el => el.textContent, reviewElement) : 'No review';

                const reviewData = new ReviewData(name, filledStars, review);
                await reviewPipeline.addData(reviewData);
            }

            await reviewPipeline.closePipeline();
            success = true;

        } catch (error) {
            console.error(`Error processing URL: ${url}. Attempt ${tries + 1} failed. Error: ${error.message}`);
            tries += 1;

            if (tries >= retries) {
                logger.error(`Max Retries exceeded for ${url}.`);

                break;
            }
        }
    }

    await page.close();
}

async function processResults(file, retries) {
    logger.info(`Processing ${file}`);
    const browser = await puppeteer.launch({ headless: true });
    const reader = [];

    fs.createReadStream(file)
        .pipe(csv())
        .on('data', (data) => reader.push(data))
        .on('end', async () => {

            for (let i = 0; i < reader.length; i += MAX_REVIEW_DATA_THREADS) {
                try {
                    const promises = [];
                    for (let j = 0; j < MAX_REVIEW_DATA_THREADS && (i + j) < reader.length; j++) {
                        promises.push(processListing(browser, reader[i + j], retries));
                    }

                    await Promise.all(promises);
                } catch (error) {
                    console.error(`Error processing row: ${error.message}`);
                }
            }

            await browser.close();
            console.log('Processing complete.');
        })
        .on('error', (error) => {
            console.error(`Error reading file: ${error.message}`);
        });
}

(async () => {
    logger.info("Crawl starting...");
    const aggregateFiles = [];
    const browser = await puppeteer.launch({ headless: true });

    for (const keyword of keywordList) {
        const filename = `${keyword.replace(/, /g, "-").replace(/ /g, "-")}.csv`;
        const dataPipeline = new DataPipeline(filename);

        try {
            const pageUrls = await findPaginationUrls(keyword);
            for (let i = 0; i < pageUrls.length; i += MAX_SEARCH_DATA_THREADS) {
                try {
                    const promises = [];
                    for (let j = 0; j < MAX_SEARCH_DATA_THREADS && (i + j) < pageUrls.length; j++) {
                        promises.push(scrapeSearchResults(browser, pageUrls[i + j], dataPipeline, MAX_RETRIES));
                    }

                    await Promise.all(promises);
                } catch (error) {
                    console.error(`Error scraping search results: ${error.message}`);
                }
            }
            await dataPipeline.closePipeline();
            aggregateFiles.push(filename);
        } catch (error) {
            logger.error(`Error processing keyword "${keyword}": ${error}`);
        }
    }

    await browser.close();
    logger.info("Crawl complete.");

    for (let file of aggregateFiles) {
        try {
            await processResults(file, MAX_RETRIES);
        } catch (error) {
            console.error(`Error processing file ${file}: ${error.message}`);
        }
    };
})();
