// =====================================================
// 📦 IMPORTS
// =====================================================
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");
const nodemailer = require("nodemailer");
const { MongoClient, ObjectId } = require("mongodb");


// =====================================================
// 🔐 CONFIG
// =====================================================
const BASE_URL = "https://appapi.chargecloud.net/v1/report/bookinghistory";

const TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI2NDJlZTBkNmU1MmIzYjg1MWNmN2MxMjkiLCJhdXRoVG9rZW5WZXJzaW9uIjoidjEiLCJpYXQiOjE3NzI1MjA3MDAsImV4cCI6MTc3MzgxNjcwMCwidHlwZSI6ImFjY2VzcyJ9.ICilt8jSUSbr7N-l2sdsOD665DOGI3as7c91QxKb4Z0";

const MONGO_URI =
  "mongodb+srv://IT_INTERN:ITINTERN123@cluster1.0pycd.mongodb.net/chargezoneprod";

const COE_MONGO_URI =
  "mongodb+srv://DarshRajputApp:tst4I6oi6m77xXJS@cluster0.jfptrcd.mongodb.net/ChargeZoneOperationEngine";

const RUN_MODE = process.argv[2] || "MORNING";

// =====================================================
// DB CLIENTS
// =====================================================
const mongoClient = new MongoClient(MONGO_URI);
const coeClient = new MongoClient(COE_MONGO_URI);

let db;
let coeDb;

// =====================================================
// LOAD PARTY CONFIG
// =====================================================
const partyConfig = JSON.parse(
  fs.readFileSync(path.join(__dirname, "partyConfig.json"), "utf8")
);

// =====================================================
// MAILER
// =====================================================
const transporter = nodemailer.createTransport({
  service: "gmail",
  auth: {
    user: "darshraj3104@gmail.com",
    pass: "ddxg ddtb fiiz mygh",
  },
});

function log(step, msg) {
  console.log(`[${new Date().toISOString()}] [${step}] ${msg}`);
}

// =====================================================
// FOLDERS
// =====================================================
const today = new Date().toISOString().split("T")[0];
const baseDir = path.join(__dirname, "DailyReports", today);
const partyDir = path.join(__dirname, "DailyReports", "PartyReports");

if (!fs.existsSync(baseDir)) fs.mkdirSync(baseDir, { recursive: true });
if (!fs.existsSync(partyDir)) fs.mkdirSync(partyDir, { recursive: true });

// =====================================================
// TIME FILTER (UNCHANGED LOGIC)
// =====================================================
const headers = {
  Authorization: `Bearer ${TOKEN}`,
  "Content-Type": "application/json",
};

const nowUTC = new Date();
const IST_OFFSET = 5.5 * 60 * 60 * 1000;
const nowIST = new Date(nowUTC.getTime() + IST_OFFSET);

const firstDayIST = new Date(
  nowIST.getFullYear(),
  nowIST.getMonth(),
  1
);

const bufferTimeIST = new Date(nowIST.getTime() - 5 * 60 * 60 * 1000);

const filterBody = {
  status_array: ["in_progress"],
  report: "bookingHistory",
  from: new Date(firstDayIST.getTime() - IST_OFFSET).toISOString(),
  to: new Date(bufferTimeIST.getTime() - IST_OFFSET).toISOString(),
  is_emsp_based_booking: false,
  is_ocpi_based_booking: true,
};

// =====================================================
// DOWNLOAD EXCEL
// =====================================================
async function downloadExcel() {
  const res = await axios({
    method: "POST",
    url: BASE_URL,
    headers,
    responseType: "arraybuffer",
    data: { ...filterBody, excel: true },
  });

  const filePath = path.join(
    baseDir,
    `bookingHistory_${Date.now()}.xlsx`
  );

  fs.writeFileSync(filePath, res.data);
  log("EXCEL", "Downloaded");
  return filePath;
}

// =====================================================
// SYNC EXCEL → COE DB (MOBILE FILTER + SKIP CLOSED)
// =====================================================
async function syncBookingsToDB(mainFile) {

  const wb = XLSX.readFile(mainFile);
  const sheet = wb.Sheets[wb.SheetNames[0]];

  const rows = XLSX.utils.sheet_to_json(sheet, {
    range: 2,
    defval: "",
  });

  const bookingIds = rows
    .map(r => r["Booking Id"])
    .filter(id => ObjectId.isValid(id))
    .map(id => new ObjectId(id));

  const bookings = await db.collection("chargerbookings")
    .find({ _id: { $in: bookingIds } })
    .toArray();

  const bookingMap = {};
  bookings.forEach(b => {
    bookingMap[String(b._id)] = b;
  });

  const bulkOps = [];
  const now = normalizeToMinute(new Date());

  for (const row of rows) {

    // 📱 MOBILE FILTER
    if (!row["Mobile No."]) continue;

    const bookingId = row["Booking Id"];
    if (!ObjectId.isValid(bookingId)) continue;

    const booking = bookingMap[bookingId];
    if (!booking) continue;

    const dbStatus = String(booking.status || "").toLowerCase();

    // ❌ Skip closed
    if (dbStatus === "completed" || dbStatus === "cancelled")
      continue;

    bulkOps.push({
      updateOne: {
        filter: { bookingId: new ObjectId(bookingId) },
        update: {
          $set: {
            bookingId: new ObjectId(bookingId),
            charger: booking.charger,
            tenant: booking.tenant,
            paymentStatus: booking.payment_status,
            connectorId: booking.connectorId,
            idTag: booking.idTag,
            bookingStartTime: booking.booking_start || booking.schedule_datetime,
            status: dbStatus,
            partyId: row["Party Id"] || "UNKNOWN",
            mobileNumber: row["Mobile No."] || null,
            updatedAt: now,
            customer_user_booked: booking.customer_user_booked || null,
            ocpi_Credential: booking.ocpiCredential || null,
            is_ocpi_based_booking: booking.is_ocpi_based_booking || false,
            is_emsp_based_booking: booking.is_emsp_based_booking || false,
            ocpi_session_id: booking.ocpi_session_id || null,
            is_script_corrected: booking.is_script_corrected || false
          },
          $setOnInsert: {
            createdAt: now,
            lifecycle: {},
            thread: { threadClosed: false },
          }
        },
        upsert: true
      }
    });
  }

  if (bulkOps.length)
    await coeDb.collection("ocpiemsp_in_progressbooking").bulkWrite(bulkOps);

  log("DB", "Initial Sync Complete");
}

// =====================================================
// 🔁 PROD ↔ COE RECONCILIATION
// =====================================================
async function reconcileStatus() {

  const collection = coeDb.collection("ocpiemsp_in_progressbooking");

  const sessions = await collection.find({
    "thread.threadClosed": { $ne: true }
  }).toArray();

  if (!sessions.length) return;

  const bookingIds = sessions.map(s => s.bookingId);

  const prodBookings = await db.collection("chargerbookings")
    .find({ _id: { $in: bookingIds } })
    .toArray();

  const prodMap = {};
  prodBookings.forEach(b => {
    prodMap[String(b._id)] = b;
  });

  const bulkOps = [];

  for (const session of sessions) {

    // 🟣 UI FINAL AUTHORITY
    if (session.uiUpdated === true && session.status === "completed") {
      continue; // Never touch this booking
    }

    const prod = prodMap[String(session.bookingId)];
    if (!prod) continue;

    const prodStatus = String(prod.status || "").toLowerCase();
    const coeStatus = String(session.status || "").toLowerCase();

    const prodScriptCorrected =
      prod.is_script_corrected === true ? true : false;

    const coeScriptCorrected =
      session.is_script_corrected === true ? true : false;

    // 🔴 PROD CLOSES SESSION
    if (prodStatus === "completed" || prodStatus === "cancelled") {

      bulkOps.push({
        updateOne: {
          filter: { bookingId: session.bookingId },
          update: {
            $set: {
              status: prodStatus,
              "thread.threadClosed": true,
              closedAt: normalizeToMinute(new Date()),
              updatedAt: normalizeToMinute(new Date())
            }
          }
        }
      });

      continue;
    }

    // 🟢 Status mismatch (only if not UI controlled)
    if (prodStatus !== coeStatus) {

      bulkOps.push({
        updateOne: {
          filter: { bookingId: session.bookingId },
          update: {
            $set: {
              status: prodStatus,
              updatedAt: normalizeToMinute(new Date())
            }
          }
        }
      });
    }
  }

  if (bulkOps.length)
    await collection.bulkWrite(bulkOps);

  log("RECON", "Status Reconciled");
}
// =====================================================
// LIFECYCLE ENGINE
// =====================================================
function normalizeToMinute(date) {
  const d = new Date(date);
  d.setSeconds(0);
  d.setMilliseconds(0);
  return d;
}
async function processLifecycleFromDB() {

  await reconcileStatus();

  const now = normalizeToMinute(new Date());
  const todayDate = now.toISOString().split("T")[0];
  const collection = coeDb.collection("ocpiemsp_in_progressbooking");

  const parties = await collection.distinct("partyId", {
    status: "in_progress",
    "thread.threadClosed": { $ne: true }
  });

  for (const partyId of parties) {

    const config = partyConfig[partyId];
    if (!config || !config.emails?.length) continue;

    const sessions = await collection.find({
      partyId,
      status: "in_progress",
      mobileNumber: { $ne: null },
      "thread.threadClosed": { $ne: true },
      $or: [
        { uiUpdated: { $exists: false } },
        { uiUpdated: false }
      ]
    }).toArray();

    const notify = [];
    const reminder1 = [];
    const finalReminder = [];

    for (const s of sessions) {

      const n = s.lifecycle?.notificationSentAt
        ? normalizeToMinute(s.lifecycle.notificationSentAt)
        : null;

      const r1 = s.lifecycle?.reminder1SentAt
        ? normalizeToMinute(s.lifecycle.reminder1SentAt)
        : null;

      if (!n) notify.push(s);
      else if (!r1 && now - n >= 86400000)
        reminder1.push(s);
      else if (
        r1 &&
        !s.lifecycle?.finalReminderSentAt &&
        now - r1 >= 86400000
      )
        finalReminder.push(s);
    }

    async function send(type, list) {

      if (!list.length) return;

      const validSessions = [];

      for (const s of list) {

        const prod = await db.collection("chargerbookings").findOne({
          _id: s.bookingId
        });

        if (!prod) continue;

        const prodStatus = String(prod.status || "").toLowerCase();

        // If closed in PROD → close in COE
        if (prodStatus === "completed" || prodStatus === "cancelled") {

          await collection.updateOne(
            { bookingId: s.bookingId },
            {
              $set: {
                status: prodStatus,
                "thread.threadClosed": true,
                closedAt: normalizeToMinute(new Date()),
                updatedAt: normalizeToMinute(new Date())
              }
            }
          );

          continue; // do NOT send mail
        }

        // Only allow active sessions
        if (prodStatus === "in_progress") {
          validSessions.push(s);
        }
      }

      if (!validSessions.length) return;

      log("MAIL", `${type} → ${partyId} (${validSessions.length})`);

      const rowsHTML = validSessions.map(s => `
    <tr>
      <td>${s.bookingId}</td>
      <td>${s.paymentStatus || "N/A"}</td>
      <td>${s.status}</td>
    </tr>
  `).join("");

      let messageIntro = "";

      if (type === "Notification") {
        messageIntro = `
    <p>Hello,</p>
    <p>
      We have identified charging session(s) that are still in <b>in-progress</b> state and for which the
      corresponding <b>Charge Detail Record (CDR)</b> has not yet been received.
    </p>
    <p>
      Kindly review the sessions listed below and push the corresponding CDRs from your system.
    </p>
  `;
      }

      if (type === "Reminder 1") {
        messageIntro = `
    <p>Hello,</p>
    <p>
      This is a reminder regarding charging session(s) that remain in <b>in-progress</b> state and for which
      the <b>Charge Detail Record (CDR)</b> is still pending.
    </p>
    <p>
      Kindly review the sessions listed below and push the corresponding CDRs from your system.
    </p>
  `;
      }

      if (type === "Final Reminder") {
        messageIntro = `
    <p>Hello,</p>
    <p>
      This is a <b>final reminder</b> regarding charging session(s) that remain in <b>in-progress</b> state
      and for which the <b>Charge Detail Record (CDR)</b> has still not been received.
    </p>
    <p>
      We request you to kindly review the sessions listed below and push the corresponding CDRs
      from your system at the earliest.
    </p>
  `;
      }

      const htmlContent = `
<div style="font-family:Arial, Helvetica, sans-serif; font-size:14px; color:#333;">
  
  ${messageIntro}

  <p><b>Session Details:</b></p>

  <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;">
    <tr style="background:#f2f2f2;">
      <th>Booking ID</th>
      <th>Payment Status</th>
      <th>Status</th>
    </tr>
    ${rowsHTML}
  </table>

  <p style="margin-top:15px;">
    Submitting the CDR will help ensure that the sessions are accurately reflected in
    <b>billing and reporting</b>.
  </p>

  <p>
    Regards,<br>
    Chargezone
  </p>

</div>
`;

      const info = await transporter.sendMail({
        from: "noreply@chargezone.co.in",
        to: config.emails.join(","),
        subject: `${partyId} | ${todayDate} | Open Sessions`,
        html: htmlContent
      });

      const threadId = info.messageId;

      for (const s of validSessions) {

        const field =
          type === "Notification"
            ? "lifecycle.notificationSentAt"
            : type === "Reminder 1"
              ? "lifecycle.reminder1SentAt"
              : "lifecycle.finalReminderSentAt";

        await collection.updateOne(
          { bookingId: s.bookingId },
          {
            $set: {
              [field]: normalizeToMinute(new Date()),
              "thread.threadId": threadId,
              "thread.threadDate": todayDate
            }
          }
        );
      }
    }

    await send("Notification", notify);
    await send("Reminder 1", reminder1);
    await send("Final Reminder", finalReminder);
  }
}

// =====================================================
// REGENERATE PARTY EXCEL FROM DB
// =====================================================
async function regenerateExcelFromDB() {

  const sessions = await coeDb.collection("ocpiemsp_in_progressbooking")
    .find({ status: "in_progress" })
    .toArray();

  const partyMap = {};

  sessions.forEach(s => {
    if (!partyMap[s.partyId]) partyMap[s.partyId] = [];

    partyMap[s.partyId].push({
      "Booking Id": s.bookingId.toString(),
      Notification: s.lifecycle?.notificationSentAt || "",
      "Reminder 1": s.lifecycle?.reminder1SentAt || "",
      "Final Reminder": s.lifecycle?.finalReminderSentAt || "",
    });
  });

  for (const partyId of Object.keys(partyMap)) {

    const filePath = path.join(partyDir, `PARTY_${partyId}.xlsx`);
    const wb = XLSX.utils.book_new();

    XLSX.utils.book_append_sheet(
      wb,
      XLSX.utils.json_to_sheet(partyMap[partyId]),
      "PartyData"
    );

    XLSX.writeFile(wb, filePath);
  }

  log("EXCEL", "Party Excel Regenerated from DB");
}

// =====================================================
// MAIN
// =====================================================
async function runAutomation() {

  await mongoClient.connect();
  await coeClient.connect();

  db = mongoClient.db("chargezoneprod");
  coeDb = coeClient.db("ChargeZoneOperationEngine");

  log("START", "DB Driven Automation Started");

  const mainFile = await downloadExcel();
  await syncBookingsToDB(mainFile);
  await processLifecycleFromDB();
  await regenerateExcelFromDB();

  log("END", "Completed");

  await mongoClient.close();
  await coeClient.close();
}

runAutomation();
