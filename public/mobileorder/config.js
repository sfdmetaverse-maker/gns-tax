// ============================================
// GLEAM & SIP — SCAN TO ORDER CONFIGURATION
// ============================================
// Edit these values to customize your setup.

const CONFIG = {
  // --- Restaurant Info ---
  restaurantName: "Gleam & Sip",
  tagline: "Crafted with care. Served with soul.",

  // --- Your Website Base URL ---
  // Used for QR code generation. Change to your actual domain.
  baseUrl: "https://gleamandsip.com/mobileorder",

  // --- Stripe Payments ---
  // Cloudflare Worker URL that creates Stripe Checkout sessions.
  // Deploy stripe-worker.js to Cloudflare Workers, then paste URL here.
  stripeWorkerUrl: "https://gleamsip-checkout.sfdmetaverse.workers.dev",

  // --- Tax Rate ---
  // 0.13 = 13% Ontario HST
  taxRate: 0.13,

  // --- Currency ---
  currency: "CAD",
  currencySymbol: "$",
};
