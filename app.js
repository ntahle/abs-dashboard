"use strict";

/*
 * Live MQTT monitor for the Intelligence Anti-Bully System.
 *
 * It uses the same connection and topics as the mobile app:
 *   testtopic/esp32/alert    alerts
 *   testtopic/esp32/status   device status
 *   testtopic/acknowledgment acknowledgement messages
 *
 * All alerts shown on the page come from MQTT. There is no sample data.
 */

(() => {
    const BROKER_URL = "wss://z181062f.ala.us-east-1.emqxsl.com:8084/mqtt";
    const MQTT_USERNAME = "syafiq";
    const MQTT_PASSWORD = "syafiq";

    const TOPICS = {
        alerts: "testtopic/esp32/alert",
        status: "testtopic/esp32/status",
        ack: "testtopic/acknowledgment",
    };

    const DEVICE_TIMEOUT_MS = 20000;
    const MAX_ALERTS = 100;
    const STORAGE_KEY = "abs_monitor_alerts_v1";
    const DEFAULT_LOCATION = "Fukuyama Dorm DML114";

    const els = {
        brokerChip: document.getElementById("brokerChip"),
        brokerState: document.getElementById("brokerState"),
        deviceChip: document.getElementById("deviceChip"),
        deviceState: document.getElementById("deviceState"),
        reconnectButton: document.getElementById("reconnectButton"),
        feedTitle: document.getElementById("feedTitle"),
        feedCount: document.getElementById("feedCount"),
        uploadCsvButton: document.getElementById("uploadCsvButton"),
        downloadCsvButton: document.getElementById("downloadCsvButton"),
        csvFileInput: document.getElementById("csvFileInput"),
        clearButton: document.getElementById("clearButton"),
        notice: document.getElementById("notice"),
        alertList: document.getElementById("alertList"),
        emptyState: document.getElementById("emptyState"),
        emptyKicker: document.getElementById("emptyKicker"),
        eventLog: document.getElementById("eventLog"),
        announcer: document.getElementById("announcer"),
    };

    let alerts = loadAlerts();
    let client = null;
    let connectionSequence = 0;
    let brokerState = "connecting";
    let deviceState = "unknown";
    let deviceLastSeenAt = 0;
    let lastBrokerError = "";
    let highlightAlertId = null;
    let noticeTimer = null;
    let alertCounter = 0;

    // A dashboard acknowledgement is echoed back by the broker because this page
    // also subscribes to the acknowledgement topic. Skip that echo once so it
    // does not acknowledge a second alert.
    let ignoreNextAck = false;
    let ignoreNextAckTimer = null;

    const ackPending = new Set();
    const events = [];

    /* ----------------------------------------------------------------------
       Helpers
       ---------------------------------------------------------------------- */

    function escapeHtml(value) {
        return String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function payloadToString(payload) {
        if (payload === null || typeof payload === "undefined") {
            return "";
        }

        if (typeof payload === "string") {
            return payload;
        }

        if (typeof payload.toString === "function") {
            try {
                const text = payload.toString("utf8");
                if (text && !/^\d+(,\d+)+$/.test(text)) {
                    return text;
                }
            } catch (_error) {
                // Fall through to TextDecoder.
            }
        }

        if (typeof Uint8Array !== "undefined" && payload instanceof Uint8Array) {
            return new TextDecoder().decode(payload);
        }

        return String(payload);
    }

    function parseStatusValue(raw) {
        let reported = String(raw || "").trim();

        try {
            const data = JSON.parse(raw);
            if (data && typeof data === "object") {
                reported = String(data.status ?? data.deviceStatus ?? data.state ?? "");
            }
        } catch (_error) {
            // Plain text status is handled below.
        }

        const value = reported.toLowerCase();

        if (value.includes("offline") || value === "disconnected" || value === "0") {
            return "offline";
        }

        // Any status message means the device is alive, unless it says offline.
        return "online";
    }

    function extractLocation(message) {
        const match = String(message || "").trim().match(/alert\s+at\s+(.+)/i);
        return match && match[1] ? match[1].trim() : "";
    }

    function parseAlertPayload(raw) {
        const text = String(raw || "");
        let data = null;

        try {
            data = JSON.parse(text);
        } catch (_error) {
            data = null;
        }

        if (!data || typeof data !== "object" || Array.isArray(data)) {
            return {
                location: extractLocation(text),
                transcription: text.trim(),
                label: "",
                confidence: null,
                reasons: [],
            };
        }

        let label = "";
        let confidence = null;
        let reasons = [];
        const analysis = data.analysis;

        if (analysis && typeof analysis === "object" && !Array.isArray(analysis)) {
            label = analysis.label ? String(analysis.label) : "";
            confidence = analysis.confidence ?? null;

            if (Array.isArray(analysis.reasons)) {
                reasons = analysis.reasons;
            } else if (typeof analysis.reasons === "string" && analysis.reasons) {
                reasons = [analysis.reasons];
            }
        } else if (typeof analysis === "string" && analysis) {
            label = analysis;
        }

        if (!label && data.label) {
            label = String(data.label);
        }

        if (confidence === null && data.confidence !== null && typeof data.confidence !== "undefined") {
            confidence = data.confidence;
        }

        if (reasons.length === 0 && Array.isArray(data.reasons)) {
            reasons = data.reasons;
        }

        const message = data.message ? String(data.message) : "";
        const transcription = data.transcription
            ? String(data.transcription)
            : data.text
                ? String(data.text)
                : message;

        const location = data.location
            ? String(data.location)
            : extractLocation(message) || extractLocation(text);

        return {
            location,
            transcription,
            label,
            confidence,
            reasons: reasons
                .filter((reason) => reason !== null && typeof reason !== "undefined")
                .map((reason) => (typeof reason === "object" ? JSON.stringify(reason) : String(reason))),
        };
    }

    function formatConfidence(value) {
        if (value === null || typeof value === "undefined" || value === "") {
            return "";
        }

        if (typeof value === "string" && value.includes("%")) {
            return value.trim();
        }

        const numeric = Number(value);
        if (!Number.isFinite(numeric)) {
            return String(value);
        }

        const percent = numeric > 0 && numeric <= 1 ? numeric * 100 : numeric;
        return `${Math.round(percent)}%`;
    }

    function prettyLabel(value) {
        const text = String(value || "").replace(/[_-]+/g, " ").trim();
        if (!text) {
            return "";
        }

        return text.charAt(0).toUpperCase() + text.slice(1);
    }

    function formatDateTime(iso) {
        const date = new Date(iso);
        if (Number.isNaN(date.getTime())) {
            return String(iso || "");
        }

        return new Intl.DateTimeFormat("en-GB", {
            day: "2-digit",
            month: "short",
            year: "numeric",
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
            hourCycle: "h23",
        }).format(date);
    }

    function formatTime(iso) {
        const date = new Date(iso);
        if (Number.isNaN(date.getTime())) {
            return "";
        }

        return new Intl.DateTimeFormat("en-GB", {
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
            hourCycle: "h23",
        }).format(date);
    }

    function makeAlertId() {
        alertCounter += 1;
        return `alert_${Date.now()}_${alertCounter}`;
    }

    /* ----------------------------------------------------------------------
       Storage
       ---------------------------------------------------------------------- */

    function normalizeStoredAlert(item) {
        if (!item || typeof item !== "object" || !item.receivedAt) {
            return null;
        }

        const receivedAt = new Date(item.receivedAt);
        if (Number.isNaN(receivedAt.getTime())) {
            return null;
        }

        return {
            id: String(item.id || makeAlertId()),
            receivedAt: receivedAt.toISOString(),
            location: String(item.location || ""),
            transcription: String(item.transcription || ""),
            label: String(item.label || ""),
            confidence: item.confidence ?? null,
            reasons: Array.isArray(item.reasons)
                ? item.reasons.map((reason) => String(reason))
                : [],
            acknowledged: Boolean(item.acknowledged),
            acknowledgedAt: item.acknowledgedAt || null,
            acknowledgedSource: item.acknowledged ? String(item.acknowledgedSource || "dashboard") : null,
            source: item.source === "csv" ? "csv" : "mqtt",
        };
    }

    function loadAlerts() {
        try {
            const raw = localStorage.getItem(STORAGE_KEY);
            const parsed = raw ? JSON.parse(raw) : [];
            if (!Array.isArray(parsed)) {
                return [];
            }

            return parsed
                .map(normalizeStoredAlert)
                .filter(Boolean)
                .sort((a, b) => new Date(b.receivedAt) - new Date(a.receivedAt))
                .slice(0, MAX_ALERTS);
        } catch (_error) {
            return [];
        }
    }

    function saveAlerts() {
        try {
            localStorage.setItem(STORAGE_KEY, JSON.stringify(alerts.slice(0, MAX_ALERTS)));
        } catch (_error) {
            // Storage may be unavailable or full. The live feed still works.
        }
    }

    /* ----------------------------------------------------------------------
       CSV import (sample data)
       ---------------------------------------------------------------------- */

    const CSV_COLUMN_ALIASES = {
        receivedAt: ["receivedat", "timestamp", "time", "datetime", "date"],
        location: ["location", "place", "room", "site"],
        transcription: ["transcription", "transcript", "text", "message", "speech"],
        label: ["label", "analysis", "category", "classification"],
        confidence: ["confidence", "score", "probability"],
        reasons: ["reasons", "reason", "notes"],
        acknowledged: ["acknowledged", "ack"],
        acknowledgedAt: ["acknowledgedat", "acktime", "acknowledgedtime"],
        acknowledgedSource: ["acknowledgedsource", "acksource"],
    };

    function normalizeCsvHeader(value) {
        return String(value || "").trim().toLowerCase().replace(/[\s_-]+/g, "");
    }

    function parseCsv(text) {
        const input = String(text || "").replace(/^\uFEFF/, "");
        const rows = [];
        let row = [];
        let field = "";
        let inQuotes = false;

        for (let index = 0; index < input.length; index += 1) {
            const character = input[index];

            if (inQuotes) {
                if (character === '"') {
                    if (input[index + 1] === '"') {
                        field += '"';
                        index += 1;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    field += character;
                }
                continue;
            }

            if (character === '"') {
                inQuotes = true;
            } else if (character === ",") {
                row.push(field);
                field = "";
            } else if (character === "\n") {
                row.push(field);
                rows.push(row);
                row = [];
                field = "";
            } else if (character === "\r") {
                if (input[index + 1] !== "\n") {
                    row.push(field);
                    rows.push(row);
                    row = [];
                    field = "";
                }
            } else {
                field += character;
            }
        }

        if (field !== "" || row.length > 0) {
            row.push(field);
            rows.push(row);
        }

        return rows.filter((cells) => cells.some((cell) => String(cell).trim() !== ""));
    }

    function mapCsvColumns(headerRow) {
        const position = {};

        headerRow.forEach((value, index) => {
            const key = normalizeCsvHeader(value);
            if (key && !(key in position)) {
                position[key] = index;
            }
        });

        const columns = {};
        Object.keys(CSV_COLUMN_ALIASES).forEach((field) => {
            columns[field] = CSV_COLUMN_ALIASES[field]
                .map((alias) => position[alias])
                .find((index) => typeof index === "number");
        });

        return columns;
    }

    function csvCell(row, index) {
        if (typeof index !== "number") {
            return "";
        }

        return String(row[index] ?? "").trim();
    }

    function parseCsvDate(value, fallback) {
        const text = String(value || "").trim();
        if (!text) {
            return fallback;
        }

        if (/^\d{10,13}$/.test(text)) {
            const numeric = Number(text);
            const date = new Date(text.length === 10 ? numeric * 1000 : numeric);
            if (!Number.isNaN(date.getTime())) {
                return date;
            }
        }

        const normalized = text.includes(" ") && !text.includes("T")
            ? text.replace(" ", "T")
            : text;
        const date = new Date(normalized);
        return Number.isNaN(date.getTime()) ? fallback : date;
    }

    function parseCsvBoolean(value) {
        return ["true", "yes", "y", "1", "acknowledged"]
            .includes(String(value || "").trim().toLowerCase());
    }

    function parseCsvReasons(value) {
        const text = String(value || "").trim();
        if (!text) {
            return [];
        }

        try {
            const parsed = JSON.parse(text);
            if (Array.isArray(parsed)) {
                return parsed
                    .filter((item) => item !== null && typeof item !== "undefined")
                    .map((item) => (typeof item === "object" ? JSON.stringify(item) : String(item)));
            }
        } catch (_error) {
            // Fall through to delimiter parsing.
        }

        return text.split(/[;|]/).map((item) => item.trim()).filter(Boolean);
    }

    function readFileText(file) {
        if (file && typeof file.text === "function") {
            return file.text();
        }

        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = () => resolve(String(reader.result || ""));
            reader.onerror = () => reject(reader.error || new Error("the file could not be read"));
            reader.readAsText(file);
        });
    }

    function importCsvAlerts(text) {
        const rows = parseCsv(text);
        if (rows.length < 2) {
            throw new Error("the file has no data rows");
        }

        const columns = mapCsvColumns(rows[0]);
        const hasRecognisedColumn = Object.values(columns)
            .some((index) => typeof index === "number");

        if (!hasRecognisedColumn) {
            throw new Error("no recognised columns. Include headers such as location, transcription, label, confidence, reasons, or timestamp");
        }

        const importNow = new Date();
        const imported = [];

        rows.slice(1).forEach((row) => {
            if (!row.some((cell) => String(cell).trim() !== "")) {
                return;
            }

            const acknowledged = parseCsvBoolean(csvCell(row, columns.acknowledged));
            const receivedAt = parseCsvDate(csvCell(row, columns.receivedAt), importNow);
            const acknowledgedAt = acknowledged
                ? parseCsvDate(csvCell(row, columns.acknowledgedAt), receivedAt)
                : null;

            imported.push({
                id: makeAlertId(),
                receivedAt: receivedAt.toISOString(),
                location: csvCell(row, columns.location),
                transcription: csvCell(row, columns.transcription),
                label: csvCell(row, columns.label),
                confidence: csvCell(row, columns.confidence) || null,
                reasons: parseCsvReasons(csvCell(row, columns.reasons)),
                acknowledged,
                acknowledgedAt: acknowledgedAt ? acknowledgedAt.toISOString() : null,
                acknowledgedSource: acknowledged
                    ? (csvCell(row, columns.acknowledgedSource) || "import")
                    : null,
                source: "csv",
            });
        });

        if (imported.length === 0) {
            throw new Error("the file has no usable data rows");
        }

        // Keep live MQTT alerts and replace sample rows from a previous upload
        // so re-uploading the same demo file does not create duplicates.
        alerts = [...imported, ...alerts.filter((alert) => alert.source !== "csv")]
            .sort((a, b) => new Date(b.receivedAt) - new Date(a.receivedAt))
            .slice(0, MAX_ALERTS);

        highlightAlertId = imported[0].id;
        saveAlerts();
        renderAlerts();

        return imported.length;
    }

    async function handleCsvFile(event) {
        const input = event.currentTarget;
        const file = input.files && input.files[0];

        if (!file) {
            return;
        }

        try {
            if (file.size > 2 * 1024 * 1024) {
                throw new Error("the file is larger than 2 MB");
            }

            const text = await readFileText(file);
            const count = importCsvAlerts(text);
            const noun = count === 1 ? "alert" : "alerts";

            showNotice(`Imported ${count} sample ${noun} from ${file.name}.`, "ok");
            logEvent(`Imported ${count} ${noun} from ${file.name}.`);
            announce(`${count} sample ${noun} imported.`);
        } catch (error) {
            const message = error && error.message ? error.message : String(error);
            showNotice(`Could not import CSV: ${message}`, "error");
            logEvent(`CSV import failed: ${message}`);
        } finally {
            input.value = "";
        }
    }

    /* ----------------------------------------------------------------------
       CSV export
       ---------------------------------------------------------------------- */

    const CSV_EXPORT_COLUMNS = [
        "timestamp",
        "location",
        "transcription",
        "label",
        "confidence",
        "reasons",
        "acknowledged",
        "acknowledgedAt",
        "acknowledgedSource",
    ];

    function csvEscape(value) {
        const text = String(value ?? "");
        return /[",\r\n]/.test(text)
            ? `"${text.replace(/"/g, '""')}"`
            : text;
    }

    function buildAlertsCsv() {
        const rows = alerts.map((alert) => [
            alert.receivedAt,
            alert.location,
            alert.transcription,
            alert.label,
            formatConfidence(alert.confidence),
            (alert.reasons || []).join("; "),
            alert.acknowledged ? "true" : "false",
            alert.acknowledgedAt || "",
            alert.acknowledgedSource || "",
        ]);

        return [CSV_EXPORT_COLUMNS, ...rows]
            .map((row) => row.map(csvEscape).join(","))
            .join("\r\n");
    }

    function downloadCsv() {
        if (alerts.length === 0) {
            return;
        }

        const now = new Date();
        const pad = (value) => String(value).padStart(2, "0");
        const stamp = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}_${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`;

        // The BOM keeps Excel from misreading non-ASCII text. The importer
        // strips it, so exported files can be uploaded again as-is.
        const blob = new Blob(["\uFEFF", buildAlertsCsv()], { type: "text/csv;charset=utf-8" });
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");

        link.href = url;
        link.download = `anti-bully-alerts-${stamp}.csv`;
        document.body.appendChild(link);
        link.click();
        link.remove();

        window.setTimeout(() => URL.revokeObjectURL(url), 1000);

        const noun = alerts.length === 1 ? "alert" : "alerts";
        showNotice(`Downloaded ${alerts.length} ${noun} as CSV.`, "ok");
        logEvent(`Exported ${alerts.length} ${noun} to CSV.`);
        announce(`${alerts.length} ${noun} downloaded as CSV.`);
    }

    /* ----------------------------------------------------------------------
       Rendering
       ---------------------------------------------------------------------- */

    function renderAlerts() {
        const waiting = alerts.filter((alert) => !alert.acknowledged).length;
        const newId = highlightAlertId;

        els.alertList.innerHTML = alerts
            .map((alert) => renderAlert(alert, alert.id === newId))
            .join("");

        highlightAlertId = null;

        if (alerts.length === 0) {
            els.feedCount.hidden = true;
        } else {
            const alertWord = alerts.length === 1 ? "alert" : "alerts";
            els.feedCount.hidden = false;
            els.feedCount.textContent = waiting > 0
                ? `${alerts.length} ${alertWord} · ${waiting} waiting for acknowledgement`
                : `${alerts.length} ${alertWord} · all acknowledged`;
        }

        els.clearButton.hidden = alerts.length === 0;
        els.downloadCsvButton.disabled = alerts.length === 0;
        document.title = waiting > 0
            ? `(${waiting}) Anti-Bully System Dashboard`
            : "Anti-Bully System Dashboard";

        updateEmptyState();
    }

    function renderAlert(alert, isNew) {
        const waiting = !alert.acknowledged;
        const sending = ackPending.has(alert.id);
        const location = alert.location || DEFAULT_LOCATION;
        const transcript = alert.transcription || "No transcription provided.";
        const label = prettyLabel(alert.label);
        const confidence = formatConfidence(alert.confidence);
        const reasons = Array.isArray(alert.reasons) ? alert.reasons.filter(Boolean) : [];
        const ackTime = alert.acknowledgedAt ? formatTime(alert.acknowledgedAt) : "";
        const ackedFromPhone = alert.acknowledgedSource === "phone";
        const ackedLocally = alert.acknowledgedSource === "import";

        const analysis = [];
        if (label) {
            analysis.push(`<span><span class="key">Label</span><strong>${escapeHtml(label)}</strong></span>`);
        }
        if (confidence) {
            analysis.push(`<span><span class="key">Confidence</span><strong>${escapeHtml(confidence)}</strong></span>`);
        }

        return `
            <article class="alert ${waiting ? "is-waiting" : "is-acknowledged"}${isNew ? " is-new" : ""}" data-alert-id="${escapeHtml(alert.id)}">
                <header class="alert-head">
                    <div>
                        <h3 class="alert-location">${escapeHtml(location)}</h3>
                        <p class="alert-time">
                            <time datetime="${escapeHtml(alert.receivedAt)}">${escapeHtml(formatDateTime(alert.receivedAt))}</time>
                        </p>
                    </div>
                    <span class="alert-state">${waiting ? "Waiting" : "Acknowledged"}</span>
                </header>

                <blockquote class="alert-transcript">${escapeHtml(transcript)}</blockquote>

                ${analysis.length ? `<div class="alert-analysis">${analysis.join("")}</div>` : ""}

                ${reasons.length ? `<p class="alert-reasons"><span class="key">Reasons</span><span>${escapeHtml(reasons.join(" · "))}</span></p>` : ""}

                <footer class="alert-foot">
                    ${waiting
                        ? `<button class="ack-button" type="button" data-ack="${escapeHtml(alert.id)}" aria-label="Acknowledge alert from ${escapeHtml(location)}"${sending ? " disabled" : ""}>${sending ? "Sending…" : "Acknowledge"}</button>`
                        : `<p class="ack-line">${ackedFromPhone ? "Acknowledged from phone" : ackedLocally ? "Acknowledged locally" : "Ack sent"}${ackTime ? ` at ${escapeHtml(ackTime)}` : ""}</p>`}
                </footer>
            </article>
        `;
    }

    function updateEmptyState() {
        const hasAlerts = alerts.length > 0;
        els.emptyState.hidden = hasAlerts;

        if (hasAlerts) {
            return;
        }

        let state = "pending";
        let kicker = "Connecting to broker";

        if (brokerState === "connected") {
            state = "connected";
            kicker = "Listening for alerts";
        } else if (["error", "offline", "disconnected", "unavailable"].includes(brokerState)) {
            state = "offline";
            kicker = "Broker not connected";
        }

        els.emptyState.dataset.state = state;
        els.emptyKicker.textContent = kicker;
    }

    function setBrokerState(state) {
        brokerState = state;
        els.brokerChip.dataset.state = state;
        els.brokerState.textContent = {
            connecting: "connecting…",
            connected: "connected",
            reconnecting: "reconnecting…",
            disconnected: "disconnected",
            offline: "offline",
            error: "connection error",
            unavailable: "unavailable",
        }[state] || state;

        els.reconnectButton.hidden = !["disconnected", "offline", "error", "unavailable"].includes(state);
        updateEmptyState();
    }

    function setDeviceState(state, shouldLog = false) {
        if (deviceState === state) {
            return;
        }

        deviceState = state;
        els.deviceChip.dataset.state = state;
        els.deviceState.textContent = state;

        if (shouldLog) {
            logEvent(state === "online" ? "Device online." : state === "offline" ? "Device offline." : "Device status unknown.");
        }
    }

    function updateDeviceStatus(raw) {
        deviceLastSeenAt = Date.now();
        setDeviceState(parseStatusValue(raw), true);
    }

    function addAlert(rawPayload) {
        const parsed = parseAlertPayload(rawPayload);
        const alert = {
            id: makeAlertId(),
            receivedAt: new Date().toISOString(),
            location: parsed.location,
            transcription: parsed.transcription,
            label: parsed.label,
            confidence: parsed.confidence,
            reasons: parsed.reasons,
            acknowledged: false,
            acknowledgedAt: null,
            acknowledgedSource: null,
            source: "mqtt",
        };

        alerts.unshift(alert);
        if (alerts.length > MAX_ALERTS) {
            alerts.length = MAX_ALERTS;
        }

        deviceLastSeenAt = Date.now();
        setDeviceState("online", true);
        saveAlerts();

        highlightAlertId = alert.id;
        renderAlerts();

        const location = alert.location || DEFAULT_LOCATION;
        announce(`New alert from ${location}.`);
        logEvent(`Alert received from ${location}.`);
    }

    function showNotice(message, tone) {
        els.notice.textContent = message;
        els.notice.className = `notice is-${tone}`;
        els.notice.hidden = false;

        window.clearTimeout(noticeTimer);
        noticeTimer = window.setTimeout(() => {
            els.notice.hidden = true;
        }, 4500);
    }

    function announce(message) {
        els.announcer.textContent = "";
        window.setTimeout(() => {
            els.announcer.textContent = message;
        }, 30);
    }

    function logEvent(message) {
        events.unshift({ at: new Date(), message });
        if (events.length > 40) {
            events.length = 40;
        }
        renderEventLog();
    }

    function renderEventLog() {
        els.eventLog.innerHTML = events
            .map((event) => `
                <li>
                    <time datetime="${escapeHtml(event.at.toISOString())}">${escapeHtml(formatTime(event.at.toISOString()))}</time>
                    <span>${escapeHtml(event.message)}</span>
                </li>
            `)
            .join("");
    }

    /* ----------------------------------------------------------------------
       MQTT
       ---------------------------------------------------------------------- */

    function connectBroker() {
        const sequence = ++connectionSequence;

        if (client) {
            const previous = client;
            client = null;
            try {
                previous.end(true);
            } catch (_error) {
                // The previous client is already gone.
            }
        }

        lastBrokerError = "";
        deviceLastSeenAt = 0;
        setDeviceState("unknown");
        setBrokerState("connecting");
        logEvent("Connecting to MQTT broker.");

        if (typeof mqtt === "undefined") {
            setBrokerState("unavailable");
            logEvent("MQTT library did not load.");
            showNotice("MQTT library is unavailable. Refresh the page to try again.", "error");
            return;
        }

        let nextClient;

        try {
            nextClient = mqtt.connect(BROKER_URL, {
                username: MQTT_USERNAME,
                password: MQTT_PASSWORD,
                reconnectPeriod: 3000,
                connectTimeout: 10000,
                clean: true,
            });
        } catch (error) {
            setBrokerState("error");
            logEvent(`Could not start MQTT: ${error.message || error}`);
            showNotice("Could not start the MQTT connection.", "error");
            return;
        }

        client = nextClient;

        const isCurrent = () => client === nextClient && sequence === connectionSequence;

        nextClient.on("connect", () => {
            if (!isCurrent()) {
                return;
            }

            lastBrokerError = "";
            setBrokerState("connected");
            logEvent("Connected to MQTT broker.");

            [TOPICS.alerts, TOPICS.status, TOPICS.ack].forEach((topic) => {
                nextClient.subscribe(topic, (error) => {
                    if (!isCurrent()) {
                        return;
                    }

                    if (error) {
                        logEvent(`Subscribe failed: ${topic}.`);
                    } else {
                        logEvent(`Subscribed to ${topic}.`);
                    }
                });
            });
        });

        nextClient.on("message", (topic, payload) => {
            if (!isCurrent()) {
                return;
            }

            const text = payloadToString(payload);

            if (topic === TOPICS.alerts) {
                addAlert(text);
                return;
            }

            if (topic === TOPICS.status) {
                updateDeviceStatus(text);
                return;
            }

            if (topic === TOPICS.ack) {
                handleAcknowledgement(text);
            }
        });

        nextClient.on("reconnect", () => {
            if (!isCurrent()) {
                return;
            }

            deviceLastSeenAt = 0;
            setDeviceState("unknown");
            setBrokerState("reconnecting");
            logEvent("Reconnecting to MQTT broker.");
        });

        nextClient.on("close", () => {
            if (!isCurrent()) {
                return;
            }

            deviceLastSeenAt = 0;
            setDeviceState("unknown");
            setBrokerState("disconnected");
            logEvent("Broker connection closed.");
        });

        nextClient.on("offline", () => {
            if (!isCurrent()) {
                return;
            }

            deviceLastSeenAt = 0;
            setDeviceState("unknown");
            setBrokerState("offline");
            logEvent("Broker went offline.");
        });

        nextClient.on("error", (error) => {
            if (!isCurrent()) {
                return;
            }

            deviceLastSeenAt = 0;
            setDeviceState("unknown");
            setBrokerState("error");

            const message = error && error.message ? error.message : String(error);
            if (message !== lastBrokerError) {
                lastBrokerError = message;
                logEvent(`Broker error: ${message}`);
            }
        });
    }

    /* ----------------------------------------------------------------------
       Actions
       ---------------------------------------------------------------------- */

    function expectOwnAckEcho() {
        ignoreNextAck = true;
        window.clearTimeout(ignoreNextAckTimer);
        ignoreNextAckTimer = window.setTimeout(() => {
            ignoreNextAck = false;
            ignoreNextAckTimer = null;
        }, 6000);
    }

    function clearOwnAckEchoExpectation() {
        ignoreNextAck = false;
        window.clearTimeout(ignoreNextAckTimer);
        ignoreNextAckTimer = null;
    }

    function handleAcknowledgement(raw) {
        let acknowledged = false;

        try {
            const data = JSON.parse(raw);
            acknowledged = Boolean(data && typeof data === "object" && (data.acknowledged === true || data.ack === true));
        } catch (_error) {
            acknowledged = String(raw).trim().toLowerCase() === "true";
        }

        if (!acknowledged) {
            return;
        }

        // The broker sends this page its own acknowledgement. The local button
        // already handles that alert, so do not consume a second waiting alert.
        if (ignoreNextAck) {
            clearOwnAckEchoExpectation();
            logEvent("Acknowledgement confirmed by the broker.");
            return;
        }

        // The acknowledgement payload has no alert ID. Treat it as an
        // acknowledgement of the newest live alert that is still waiting.
        // Imported sample alerts are local-only and are never targeted.
        const target = alerts.find((alert) => !alert.acknowledged && alert.source !== "csv");

        if (!target) {
            logEvent("Phone acknowledgement received; no waiting alerts.");
            showNotice("Acknowledgement received from phone. No waiting alerts.", "ok");
            return;
        }

        target.acknowledged = true;
        target.acknowledgedAt = new Date().toISOString();
        target.acknowledgedSource = "phone";
        saveAlerts();
        renderAlerts();

        const location = target.location || DEFAULT_LOCATION;
        showNotice(`Acknowledged from phone: ${location}.`, "ok");
        logEvent(`Phone acknowledged alert from ${location}.`);
        announce(`Alert from ${location} acknowledged from phone.`);
    }

    function acknowledgeAlert(alertId) {
        const alert = alerts.find((item) => item.id === alertId);
        if (!alert || alert.acknowledged || ackPending.has(alertId)) {
            return;
        }

        // Imported sample alerts are acknowledged locally only, so they never
        // publish an acknowledgement that could silence a real device.
        if (alert.source === "csv") {
            const location = alert.location || DEFAULT_LOCATION;
            alert.acknowledged = true;
            alert.acknowledgedAt = new Date().toISOString();
            alert.acknowledgedSource = "import";
            saveAlerts();
            renderAlerts();
            showNotice("Imported alert acknowledged locally.", "ok");
            logEvent(`Imported alert from ${location} acknowledged locally.`);
            announce(`Imported alert from ${location} acknowledged.`);
            return;
        }

        if (!client || !client.connected) {
            showNotice("Broker is not connected. The acknowledgement was not sent.", "error");
            logEvent("Acknowledgement not sent: broker is disconnected.");
            return;
        }

        ackPending.add(alertId);
        expectOwnAckEcho();
        renderAlerts();

        let settled = false;
        let timeout = null;

        const finish = (error) => {
            if (settled) {
                return;
            }

            settled = true;
            window.clearTimeout(timeout);
            ackPending.delete(alertId);

            if (error) {
                clearOwnAckEchoExpectation();
                renderAlerts();
                showNotice(`Acknowledgement failed: ${error.message || error}`, "error");
                logEvent(`Acknowledgement failed: ${error.message || error}`);
                return;
            }

            const current = alerts.find((item) => item.id === alertId);
            if (current) {
                current.acknowledged = true;
                current.acknowledgedAt = new Date().toISOString();
                current.acknowledgedSource = "dashboard";
                saveAlerts();
            }

            renderAlerts();
            showNotice("Acknowledgement sent.", "ok");
            logEvent(`Acknowledgement sent for ${current ? current.location || DEFAULT_LOCATION : DEFAULT_LOCATION}.`);
            announce(current ? `Alert from ${current.location || DEFAULT_LOCATION} acknowledged.` : "Acknowledgement sent.");
        };

        timeout = window.setTimeout(() => {
            finish(new Error("the broker did not confirm in time"));
        }, 8000);

        try {
            client.publish(
                TOPICS.ack,
                JSON.stringify({ acknowledged: true }),
                { qos: 0 },
                (error) => finish(error)
            );
        } catch (error) {
            finish(error);
        }
    }

    function clearFeed() {
        if (alerts.length === 0) {
            return;
        }

        if (!window.confirm("Clear all received alerts from this browser?")) {
            return;
        }

        alerts = [];
        ackPending.clear();
        saveAlerts();
        renderAlerts();
        showNotice("Feed cleared.", "ok");
        logEvent("Alert feed cleared.");
        announce("Alert feed cleared.");
    }

    function deviceHeartbeatCheck() {
        if (brokerState !== "connected" || deviceLastSeenAt === 0) {
            return;
        }

        if (Date.now() - deviceLastSeenAt > DEVICE_TIMEOUT_MS && deviceState !== "offline") {
            setDeviceState("offline", true);
            logEvent("Device marked offline after 20 seconds without a message.");
        }
    }

    /* ----------------------------------------------------------------------
       Events and startup
       ---------------------------------------------------------------------- */

    function installEvents() {
        els.alertList.addEventListener("click", (event) => {
            const button = event.target.closest("[data-ack]");
            if (!button) {
                return;
            }

            acknowledgeAlert(button.getAttribute("data-ack"));
        });

        els.reconnectButton.addEventListener("click", () => {
            connectBroker();
        });

        els.uploadCsvButton.addEventListener("click", () => {
            els.csvFileInput.click();
        });

        els.csvFileInput.addEventListener("change", handleCsvFile);

        els.downloadCsvButton.addEventListener("click", downloadCsv);

        els.clearButton.addEventListener("click", clearFeed);
    }

    renderAlerts();
    installEvents();
    logEvent("Monitor loaded.");
    connectBroker();

    window.setInterval(deviceHeartbeatCheck, 5000);
})();
