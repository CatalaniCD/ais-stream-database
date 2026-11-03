import WebSocket from 'ws';
import Database from 'better-sqlite3';
import dotenv from 'dotenv';

dotenv.config();

// 1. TypeScript Interfaces for AISStream Payloads
interface AISStreamMetadata {
    MMSI: number;
    ShipName?: string;
    Latitude?: number;
    Longitude?: number;
    Destination?: string;
    Time_UTC?: string;
}

interface PositionReportPayload {
    MessageID: number;
    RepeatIndicator: number;
    UserID: number; // MMSI
    NavigationalStatus: number;
    Sog: number; // Speed Over Ground (knots)
    PositionAccuracy: boolean;
    Longitude: number;
    Latitude: number;
    Cog: number; // Course Over Ground
    TrueHeading: number;
    Timestamp: number;
}

interface AISStreamMessage {
    MessageType: string;
    MetaData: AISStreamMetadata;
    Message?: {
        PositionReport?: PositionReportPayload;
        [key: string]: any;
    };
}

// 2. Configuration
const API_KEY = process.env.AISSTREAM_API_KEY;
const DB_PATH = process.env.DB_PATH || 'vessels.db';

if (!API_KEY || API_KEY === 'your_actual_api_key_here') {
    console.error('❌ Error: Please set a valid AISSTREAM_API_KEY in your .env file.');
    process.exit(1);
}

// 3. Initialize SQLite Database
const db = new Database(DB_PATH);

// Enable Write-Ahead Logging (WAL) for concurrent reads/writes during high-throughput streaming
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');
db.pragma('busy_timeout = 5000');

// Create table structure
db.exec(`
    CREATE TABLE IF NOT EXISTS vessel_positions (
        mmsi INTEGER PRIMARY KEY,
        ship_name TEXT,
        latitude REAL,
        longitude REAL,
        speed REAL,
        heading REAL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    );
`);

// Prepared statement for ultra-fast performance
const upsertStatement = db.prepare(`
    INSERT INTO vessel_positions (mmsi, ship_name, latitude, longitude, speed, heading, timestamp)
    VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
    ON CONFLICT(mmsi) DO UPDATE SET
        ship_name = COALESCE(NULLIF(excluded.ship_name, 'Unknown'), vessel_positions.ship_name),
        latitude = excluded.latitude,
        longitude = excluded.longitude,
        speed = excluded.speed,
        heading = excluded.heading,
        timestamp = datetime('now')
`);

// 4. WebSocket Client & Reconnection Lifecycle
function connectWebSocket(): void {
    const ws = new WebSocket('wss://stream.aisstream.io/v0/stream');

    ws.on('open', () => {
        console.log(`🟢 Connected to AISStream! Ingesting data into ${DB_PATH}...`);
        
        const subscriptionMessage = {
            APIKey: API_KEY,
            // Bounding box filter (e.g., configured for regional waters / Paraná River corridor or global if desired)
	    BoundingBoxes: [[[-35.10, -61.05], [-32.50, -56.30]], [[-34.25, -60.45], [-33.20, -58.85]], [[-35.00, -58.60], [-34.30, -56.30]]],
            FilterMessageTypes: ["PositionReport"]
        };
        
        ws.send(JSON.stringify(subscriptionMessage));
    });

    ws.on('message', (rawData: WebSocket.RawData) => {
        try {
            const parsedData: AISStreamMessage = JSON.parse(rawData.toString());
            
	    console.log(parsedData);

            if (parsedData.MessageType === "PositionReport" && parsedData.Message?.PositionReport) {
                const metadata = parsedData.MetaData;
                const payload = parsedData.Message.PositionReport;

                if (metadata?.MMSI && payload?.Latitude !== undefined && payload?.Longitude !== undefined) {
                    const shipName = metadata.ShipName?.trim() || "Unknown";
                    
                    upsertStatement.run(
                        metadata.MMSI,
                        shipName,
                        payload.Latitude,
                        payload.Longitude,
                        payload.Sog,
                        payload.TrueHeading ?? payload.Cog
                    );
                }
            }
        } catch (err) {
            console.error("⚠️ Data processing failure:", err);
        }
    });

    ws.on('error', (err: Error) => {
        console.error("❌ WebSocket Error:", err.message);
    });

    ws.on('close', (code, reason) => {
        console.warn(`⚠️ Stream disconnected (Code: ${code}, Reason: ${reason.toString() || 'None'}). Reconnecting in 5 seconds...`);
        setTimeout(connectWebSocket, 5000);
    });
}

// 5. Graceful Shutdown Handler
const shutdown = () => {
    console.log("\n🛑 Shutting down ingestion service...");
    try {
        db.close();
        console.log("Database connection closed cleanly.");
    } catch (e) {
        console.error("Error closing database:", e);
    }
    process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// Start the ingestion worker
connectWebSocket();
