// ============================================
// GLEAM & SIP — SCAN TO ORDER
// Menu Data, Cart, and Ordering Logic
// ============================================

// ---- MODIFIER GROUP DEFINITIONS ----
// Shared modifier groups referenced by menu items.
// Upcharges configured 2026-03-28.
const MOD_MILK = {
  id: "milk", name: "Milk Selection", required: true, maxSelect: 1,
  options: [
    { id: "oat", name: "Oat", priceAdj: 0 },
    { id: "almond", name: "Almond", priceAdj: 0 },
    { id: "organic", name: "Organic Milk", priceAdj: 0 },
    { id: "organic-2pct", name: "Organic 2% Milk", priceAdj: 0 },
    { id: "soy", name: "Soy", priceAdj: 0 },
    { id: "coconut", name: "Coconut", priceAdj: 0 },
    { id: "black", name: "Black (no milk)", priceAdj: 0 },
  ]
};
const MOD_SIZE = {
  id: "size", name: "Size", required: true, maxSelect: 1,
  options: [
    { id: "regular", name: "Regular", priceAdj: 0 },
    { id: "large", name: "Large", priceAdj: 1.00 },
  ]
};
const MOD_TEMP = {
  id: "temp", name: "Temperature", required: true, maxSelect: 1,
  options: [
    { id: "hot", name: "Hot", priceAdj: 0 },
    { id: "cold", name: "Cold (1/3 ice, more milk)", priceAdj: 1.00 },
    { id: "iced", name: "Iced", priceAdj: 0 },
  ]
};
const MOD_SYRUP = {
  id: "syrup", name: "Syrup", required: false, maxSelect: 1,
  options: [
    { id: "simple", name: "Simple", priceAdj: 1.00 },
    { id: "agave", name: "Agave", priceAdj: 1.00 },
    { id: "maple", name: "Maple Syrup", priceAdj: 1.00 },
    { id: "pumpkin-spice", name: "Pumpkin Spice", priceAdj: 1.00 },
    { id: "vanilla", name: "Vanilla", priceAdj: 1.00 },
    { id: "butterscotch", name: "Butterscotch", priceAdj: 1.00 },
    { id: "pomegranate", name: "Pomegranate", priceAdj: 1.00 },
    { id: "caramel", name: "Caramel", priceAdj: 1.00 },
    { id: "monk-fruit", name: "Monk Fruit (0 Cal)", priceAdj: 1.00 },
    { id: "irish-cream", name: "Irish Cream", priceAdj: 1.00 },
  ]
};
const MOD_COFFEE_BEAN = {
  id: "bean", name: "Coffee Bean", required: true, maxSelect: 1,
  options: [
    { id: "guji", name: "Ethiopia Guji G2", priceAdj: 0 },
    { id: "decaf", name: "Mexico Decaf (Swiss Water)", priceAdj: 0 },
    { id: "lychee", name: "Columbia Lychee", priceAdj: 2.00 },
  ]
};
const MOD_EXTRA_SHOT = {
  id: "extra-shot", name: "Extra Shot", required: false, maxSelect: 1,
  options: [
    { id: "1-shot", name: "1 Espresso", priceAdj: 2.00 },
    { id: "2-shots", name: "2 Espressos", priceAdj: 4.00 },
  ]
};
const MOD_FOAM = {
  id: "foam", name: "Foam", required: false, maxSelect: 1,
  options: [
    { id: "coconut-foam", name: "Coconut Cold Foam", priceAdj: 2.00 },
    { id: "misto", name: "Misto", priceAdj: 2.00 },
  ]
};
const MOD_TEA = {
  id: "tea", name: "Tea Selection", required: true, maxSelect: 1,
  options: [
    { id: "earl-grey", name: "Earl Grey", priceAdj: 0 },
    { id: "jasmine-green", name: "Jasmine Green Tea", priceAdj: 0 },
    { id: "english-breakfast", name: "English Breakfast", priceAdj: 0 },
    { id: "green-tea", name: "Green Tea", priceAdj: 0 },
    { id: "orange-pekoe", name: "Orange Pekoe", priceAdj: 0 },
    { id: "oolong", name: "Oolong Tea", priceAdj: 0 },
    { id: "premium-oolong", name: "Premium Oolong", priceAdj: 0 },
    { id: "puer", name: "Puer Tea", priceAdj: 0 },
  ]
};
const MOD_HERBAL_TEA = {
  id: "herbal-tea", name: "Herbal Tea Selection", required: true, maxSelect: 1,
  options: [
    { id: "chamomile", name: "Chamomile", priceAdj: 0 },
    { id: "rose", name: "Rose", priceAdj: 0 },
    { id: "lavender", name: "Lavender", priceAdj: 0 },
    { id: "hibiscus", name: "Hibiscus/Roselle", priceAdj: 0 },
    { id: "peppermint", name: "Peppermint", priceAdj: 0 },
    { id: "osmanthus", name: "Osmanthus", priceAdj: 0 },
    { id: "rooibos", name: "Rooibos", priceAdj: 0 },
  ]
};
const MOD_TEA_LATTE_SHOT = {
  id: "tea-latte-shot", name: "Tea Latte Flavour", required: true, maxSelect: 1,
  options: [
    { id: "chai", name: "Chai", priceAdj: 0 },
    { id: "london-fog", name: "London Fog (Earl Grey)", priceAdj: 0 },
    { id: "jasmine", name: "Jasmine Green Tea", priceAdj: 0 },
  ]
};
const MOD_MATCHA_CHOICE = {
  id: "matcha-choice", name: "Matcha Choice", required: true, maxSelect: 1,
  options: [
    { id: "house", name: "House", priceAdj: 0 },
    { id: "uji", name: "Uji", priceAdj: 2.00 },
    { id: "shizouka", name: "Shizouka", priceAdj: 2.00 },
    { id: "kagoshima", name: "Kagoshima", priceAdj: 2.00 },
  ]
};
const MOD_EXTRA_MATCHA = {
  id: "extra-matcha", name: "Extra Matcha", required: false, maxSelect: 1,
  options: [
    { id: "2.5g", name: "2.5g Matcha", priceAdj: 2.00 },
    { id: "5g", name: "5g Matcha", priceAdj: 2.00 },
  ]
};
const MOD_HOJICHA_CHOICE = {
  id: "hojicha-choice", name: "Hojicha Choice", required: true, maxSelect: 1,
  options: [
    { id: "house", name: "House", priceAdj: 0 },
    { id: "bohojicha", name: "Bohojicha", priceAdj: 0 },
  ]
};
const MOD_SMOOTHIE_BOOSTER = {
  id: "booster", name: "Add Booster", required: false, maxSelect: 1,
  options: [
    { id: "brown-rice", name: "Brown Rice Protein", priceAdj: 2.00 },
    { id: "hemp", name: "Hemp Protein", priceAdj: 2.00 },
    { id: "matcha", name: "Matcha", priceAdj: 2.00 },
    { id: "espresso", name: "Espresso", priceAdj: 2.00 },
  ]
};
const MOD_BREAD = {
  id: "bread", name: "Choice of Bread", required: true, maxSelect: 1,
  options: [
    { id: "sandwich", name: "Sandwich", priceAdj: 0 },
    { id: "sourdough", name: "Sourdough", priceAdj: 0 },
  ]
};
const MOD_ONIGIRI_FLAVOUR = {
  id: "onigiri-flavour", name: "Choose Flavour", required: true, maxSelect: 1,
  options: [
    { id: "pumpkin-chestnut", name: "Pumpkin Chestnut", priceAdj: 0 },
    { id: "kimchi-cheddar", name: "Kimchi Cheddar", priceAdj: 0 },
    { id: "truffle-mushroom", name: "Truffle Mushroom", priceAdj: 0 },
  ]
};
const MOD_FERMENTATION_SHOTS = {
  id: "ferm-shots", name: "Add Shot", required: false, maxSelect: 1,
  options: [
    { id: "matcha", name: "Matcha", priceAdj: 2.00 },
    { id: "ginger", name: "Ginger", priceAdj: 2.00 },
  ]
};

// ---- COMBO MODIFIER GROUPS ----
const MOD_COMBO_DRINK = {
  id: "combo-drink", name: "Choose a Drink", required: true, maxSelect: 1,
  options: [
    { id: "matcha-latte", name: "Matcha Latte", priceAdj: 0 },
    { id: "latte", name: "Latte", priceAdj: 0 },
    { id: "americano", name: "Americano", priceAdj: 0 },
    { id: "cappuccino", name: "Cappuccino", priceAdj: 0 },
    { id: "flat-white", name: "Flat White", priceAdj: 0 },
    { id: "nitro-cold-brew", name: "Nitro Cold Brew", priceAdj: 0 },
    { id: "espresso", name: "Espresso", priceAdj: 0 },
    { id: "mocha", name: "Mocha", priceAdj: 0 },
    { id: "cortado", name: "Cortado", priceAdj: 0 },
    { id: "hojicha-latte", name: "Hojicha Latte", priceAdj: 0 },
    { id: "hand-drip", name: "Hand-Drip Coffee", priceAdj: 0 },
    { id: "flojo", name: "Flojo", priceAdj: 0 },
    { id: "chai-latte", name: "Chai Latte", priceAdj: 0 },
    { id: "london-fog", name: "London Fog", priceAdj: 0 },
    { id: "jasmine-green", name: "Jasmine Green Tea", priceAdj: 0 },
    { id: "green-tea", name: "Green Tea", priceAdj: 0 },
    { id: "herbal-tea", name: "Herbal Tea", priceAdj: 0 },
    { id: "golden-latte", name: "Golden Latte", priceAdj: 0 },
    { id: "hot-choco", name: "Hot Chocolate", priceAdj: 0 },
  ]
};
const MOD_COMBO_SANDWICH = {
  id: "combo-sandwich", name: "Choose a Sandwich", required: true, maxSelect: 1,
  options: [
    { id: "avo-cucumber", name: "Avo & Cucumber", priceAdj: 0 },
    { id: "creamy-mushroom", name: "Creamy Mushroom", priceAdj: 0 },
    { id: "egg-salad", name: "Plant-based Egg Salad", priceAdj: 0 },
  ]
};
const MOD_COMBO_SWEET = {
  id: "combo-sweet", name: "Choose your Sweet", required: true, maxSelect: 1,
  options: [
    { id: "chia-pudding", name: "Chia Seed Pudding", priceAdj: 0 },
    { id: "apple-muffin", name: "Apple Muffin", priceAdj: 0 },
    { id: "banana-muffin", name: "Banana Muffin", priceAdj: 0 },
    { id: "blueberry-scone", name: "Blueberry Scone", priceAdj: 0 },
    { id: "cinnamon-scone", name: "Cinnamon Scone", priceAdj: 0 },
    { id: "cheesy-scone", name: "Cheesy Scone", priceAdj: 0 },
  ]
};

// ---- GROUP & CATEGORY ORDER ----
const GROUP_ORDER = ["Drink", "Food", "Combo", "Take Home"];
const CATEGORY_TO_GROUP = {
  "Matcha": "Drink",
  "Coffee": "Drink",
  "Tea": "Drink",
  "Smoothies": "Drink",
  "Soda & Cold": "Drink",
  "Sandwiches": "Food",
  "Bites": "Food",
  "Sweets": "Food",
  "Combos": "Combo",
  "Retail": "Take Home",
};

// ---- MENU DATA ----
// Fallback menu data — live data is fetched from Cloudflare KV via the Worker.
// This array is used only if the Worker is unreachable.
const FALLBACK_MENU = [
  // ─── MATCHA ───
  {
    id: "matcha-latte",
    name: "Matcha Latte",
    description: "Hand-whisked ceremonial matcha, your choice of milk",
    price: 5.99,
    category: "Matcha",
    image: "images/matcha-latte.png",
    modifierGroups: [MOD_TEMP, MOD_EXTRA_MATCHA, MOD_MATCHA_CHOICE, MOD_SIZE, MOD_SYRUP, MOD_MILK],
  },
  {
    id: "double-matcha",
    name: "Double Matcha",
    description: "Extra-strength hand-whisked matcha, bold and vibrant",
    price: 8.99,
    category: "Matcha",
    image: "images/double-matcha.png",
    modifierGroups: [MOD_MATCHA_CHOICE, MOD_MILK],
  },
  {
    id: "matcha-clear",
    name: "Hand-whisked Matcha Clear",
    description: "Pure matcha whisked with water — nothing else",
    price: 5.99,
    category: "Matcha",
    image: "images/matcha-clear.png",
  },
  {
    id: "matcha-coconut-cloud",
    name: "Matcha Coconut Cloud",
    description: "Matcha layered with coconut cold foam",
    price: 8.99,
    category: "Matcha",
    image: "images/matcha-coconut-cloud.png",
    modifierGroups: [MOD_EXTRA_MATCHA, MOD_MATCHA_CHOICE, MOD_SIZE],
  },
  {
    id: "strawberry-matcha",
    name: "Strawberry Matcha",
    description: "Iced matcha with strawberry purée layers",
    price: 8.99,
    category: "Matcha",
    image: "images/strawberry-matcha.png",
    modifierGroups: [MOD_MATCHA_CHOICE, MOD_MILK],
  },
  {
    id: "strawberry-hojicha",
    name: "Strawberry Hojicha",
    description: "Iced hojicha with strawberry purée layers",
    price: 8.99,
    category: "Matcha",
    image: "images/strawberry-matcha.png",
    modifierGroups: [MOD_MILK],
  },
  {
    id: "evoo-matcha",
    name: "EVOO Matcha",
    description: "Matcha blended with organic extra virgin olive oil",
    price: 12.99,
    category: "Matcha",
    image: "images/matcha-latte.png",
  },
  {
    id: "awakening-matcha",
    name: "Awakening Matcha",
    description: "Seasonal specialty matcha with unique flavor pairing",
    price: 8.00,
    category: "Matcha",
    image: "images/double-matcha.png",
    modifierGroups: [MOD_MILK],
  },

  // ─── COFFEE ───
  {
    id: "americano",
    name: "Americano",
    description: "Single-origin espresso with hot water",
    price: 4.99,
    category: "Coffee",
    image: "images/americano.png",
    modifierGroups: [MOD_EXTRA_SHOT, MOD_SIZE, MOD_FOAM, MOD_COFFEE_BEAN, MOD_TEMP],
  },
  {
    id: "caffe-latte",
    name: "Caffè Latte",
    description: "Espresso with steamed milk, latte art",
    price: 5.99,
    category: "Coffee",
    image: "images/caffe-latte.png",
    modifierGroups: [MOD_TEMP, MOD_EXTRA_SHOT, MOD_SIZE, MOD_SYRUP, MOD_MILK, MOD_COFFEE_BEAN],
  },
  {
    id: "cappuccino",
    name: "Cappuccino",
    description: "Espresso with thick, velvety milk foam",
    price: 5.99,
    category: "Coffee",
    image: "images/cappuccino.png",
    modifierGroups: [MOD_EXTRA_SHOT, MOD_SYRUP, MOD_MILK, MOD_COFFEE_BEAN],
  },
  {
    id: "flat-white",
    name: "Flat White",
    description: "Double ristretto with silky microfoam",
    price: 5.99,
    category: "Coffee",
    image: "images/flat-white.png",
    modifierGroups: [MOD_MILK],
  },
  {
    id: "cortado",
    name: "Cortado",
    description: "Equal parts espresso and warm milk",
    price: 5.99,
    category: "Coffee",
    image: "images/cortado.png",
    modifierGroups: [MOD_MILK],
  },
  {
    id: "mocha",
    name: "Mocha",
    description: "Espresso, chocolate, steamed milk, cocoa",
    price: 6.99,
    category: "Coffee",
    image: "images/mocha.png",
    modifierGroups: [MOD_TEMP, MOD_EXTRA_SHOT, MOD_SIZE, MOD_MILK],
  },
  {
    id: "spanish-latte",
    name: "Spanish Latte",
    description: "Espresso with condensed milk, sweet and creamy",
    price: 6.99,
    category: "Coffee",
    image: "images/spanish-latte.png",
    modifierGroups: [MOD_MILK],
  },
  {
    id: "mushroom-mocha",
    name: "Mushroom Mocha",
    description: "Espresso, adaptogenic mushroom blend, chocolate",
    price: 12.99,
    category: "Coffee",
    image: "images/mocha.png",
  },
  {
    id: "golden-latte",
    name: "Golden Latte",
    description: "Turmeric, ginger, cinnamon with steamed milk",
    price: 5.99,
    category: "Coffee",
    image: "images/golden-latte.png",
    modifierGroups: [MOD_SIZE, MOD_MILK],
  },
  {
    id: "drip-coffee",
    name: "Handcrafted Drip Coffee",
    description: "Single-origin, freshly ground to order (8oz)",
    price: 2.99,
    category: "Coffee",
    image: "images/drip-coffee.png",
  },
  {
    id: "awakening-americano",
    name: "Awakening Americano",
    description: "Seasonal specialty americano",
    price: 7.00,
    category: "Coffee",
    image: "images/americano.png",
  },
  {
    id: "nitro-cold-brew",
    name: "Nitro Cold Brew",
    description: "Nitrogen-infused cold brew, creamy cascade",
    price: 5.99,
    category: "Coffee",
    image: "images/nitro-cold-brew.png",
    modifierGroups: [MOD_SIZE, MOD_FOAM],
  },
  {
    id: "oj-americano",
    name: "OJ Americano",
    description: "Espresso meets fresh-pressed orange juice, iced",
    price: 7.99,
    category: "Coffee",
    image: "images/oj-americano.png",
    modifierGroups: [MOD_COFFEE_BEAN],
  },
  {
    id: "espresso",
    name: "Espresso",
    description: "Single or double shot, single-origin beans",
    price: 3.99,
    category: "Coffee",
    image: "images/americano.png",
    modifierGroups: [MOD_FOAM, MOD_SIZE],
  },

  // ─── TEA ───
  {
    id: "tea",
    name: "Tea",
    description: "Choose from our selection of premium loose-leaf teas",
    price: 4.99,
    category: "Tea",
    image: "images/tea.png",
    modifierGroups: [MOD_TEMP, MOD_SIZE, MOD_SYRUP, MOD_TEA],
  },
  {
    id: "hojicha-latte",
    name: "Hojicha Latte",
    description: "Roasted Japanese green tea with steamed milk",
    price: 5.99,
    category: "Tea",
    image: "images/hojicha-latte.png",
    modifierGroups: [MOD_TEMP, MOD_SIZE, MOD_SYRUP, MOD_MILK, MOD_HOJICHA_CHOICE],
  },
  {
    id: "tea-latte",
    name: "Tea Latte",
    description: "Your choice of tea with steamed milk",
    price: 5.99,
    category: "Tea",
    image: "images/tea-latte.png",
    modifierGroups: [MOD_TEMP, MOD_SIZE, MOD_SYRUP, MOD_TEA_LATTE_SHOT, MOD_MILK],
  },
  {
    id: "herbal-tea",
    name: "Herbal Tea",
    description: "Caffeine-free herbal infusion with botanicals",
    price: 4.99,
    category: "Tea",
    image: "images/herbal-tea.png",
    modifierGroups: [MOD_TEMP, MOD_SIZE, MOD_SYRUP, MOD_HERBAL_TEA],
  },
  {
    id: "hot-choco",
    name: "Hot Chocolate",
    description: "Rich cocoa with steamed milk",
    price: 5.99,
    category: "Tea",
    image: "images/hot-choco.png",
    modifierGroups: [MOD_TEMP, MOD_SYRUP, MOD_MILK, MOD_SIZE],
  },
  {
    id: "babycinno",
    name: "Babycinno",
    description: "Steamed milk with a hint of cocoa powder",
    price: 2.99,
    category: "Tea",
    image: "images/hot-choco.png",
    modifierGroups: [MOD_MILK, MOD_SYRUP],
  },

  // ─── SMOOTHIES ───
  {
    id: "smoothie-red",
    name: "Red",
    description: "Berry blend smoothie, vibrant and refreshing",
    price: 10.99,
    category: "Smoothies",
    image: "images/smoothie-red.png",
    modifierGroups: [MOD_SMOOTHIE_BOOSTER],
  },
  {
    id: "smoothie-green",
    name: "Green",
    description: "Spinach, banana, and superfood greens",
    price: 10.99,
    category: "Smoothies",
    image: "images/smoothie-green.png",
    modifierGroups: [MOD_SMOOTHIE_BOOSTER],
  },
  {
    id: "smoothie-blue",
    name: "Blue",
    description: "Butterfly pea flower, blueberry, tropical blend",
    price: 10.99,
    category: "Smoothies",
    image: "images/smoothie-blue.png",
    modifierGroups: [MOD_SMOOTHIE_BOOSTER],
  },
  {
    id: "smoothie-black",
    name: "Black",
    description: "Activated charcoal, black sesame, banana",
    price: 10.99,
    category: "Smoothies",
    image: "images/smoothie-black.png",
    modifierGroups: [MOD_SMOOTHIE_BOOSTER],
  },
  {
    id: "smoothie-white",
    name: "White",
    description: "Coconut, vanilla, banana, protein blend",
    price: 10.99,
    category: "Smoothies",
    image: "images/smoothie-white.png",
    modifierGroups: [MOD_SMOOTHIE_BOOSTER],
  },
  {
    id: "pollinator-glow",
    name: "Pollinator Glow",
    description: "Bee pollen, honey, tropical superfoods",
    price: 9.99,
    category: "Smoothies",
    image: "images/smoothie-green.png",
  },

  // ─── SODA & COLD DRINKS ───
  {
    id: "yuzu-mist",
    name: "Yuzu Mist",
    description: "Yuzu purée with sparkling soda",
    price: 7.99,
    category: "Soda & Cold",
    image: "images/soda.png",
  },
  {
    id: "blue-bloom",
    name: "Blue Bloom",
    description: "Butterfly pea flower lemonade, colour-changing",
    price: 7.99,
    category: "Soda & Cold",
    image: "images/soda.png",
  },
  {
    id: "crimson-sparkler",
    name: "Crimson Sparkler",
    description: "Raspberry hibiscus sparkling soda",
    price: 7.99,
    category: "Soda & Cold",
    image: "images/soda.png",
  },
  {
    id: "velvet-blush",
    name: "Velvet Blush",
    description: "Strawberry rose sparkling soda",
    price: 7.99,
    category: "Soda & Cold",
    image: "images/soda.png",
  },
  {
    id: "strawberry-choco",
    name: "Strawberry Chocolate",
    description: "Strawberry and chocolate blended iced drink",
    price: 8.99,
    category: "Soda & Cold",
    image: "images/smoothie-red.png",
  },
  {
    id: "root-beer",
    name: "House Fermented Root Beer",
    description: "Small-batch, naturally fermented",
    price: 5.99,
    category: "Soda & Cold",
    image: "images/kombucha.png",
  },
  {
    id: "kombucha",
    name: "House Fermented Kombucha",
    description: "Probiotic-rich, house-brewed",
    price: 8.99,
    category: "Soda & Cold",
    image: "images/kombucha.png",
    modifierGroups: [MOD_FERMENTATION_SHOTS],
  },
  {
    id: "matcha-kombucha",
    name: "Matcha Kombucha",
    description: "Matcha-infused fermented kombucha",
    price: 10.99,
    category: "Soda & Cold",
    image: "images/matcha-kombucha.png",
  },
  {
    id: "ramune-soda",
    name: "Ramune Soda",
    description: "Classic Japanese marble soda",
    price: 4.99,
    category: "Soda & Cold",
    image: "images/soda.png",
  },
  {
    id: "smart-water",
    name: "Smart Water",
    description: "Bottled vapor-distilled water",
    price: 2.99,
    category: "Soda & Cold",
    image: "images/smart-water.png",
  },
  {
    id: "seltzer",
    name: "Seltzer Water",
    description: "Sparkling water",
    price: 2.99,
    category: "Soda & Cold",
    image: "images/smart-water.png",
  },

  // ─── SANDWICHES ───
  {
    id: "avo-cucumber",
    name: "Avo & Cucumber",
    description: "Avocado, cucumber, greens on house-baked sourdough",
    price: 13.99,
    category: "Sandwiches",
    image: "images/avo-cucumber.png",
    modifierGroups: [MOD_BREAD],
  },
  {
    id: "creamy-mushroom",
    name: "Creamy Mushroom",
    description: "Savory mushroom cream on house-baked sourdough",
    price: 13.99,
    category: "Sandwiches",
    image: "images/creamy-mushroom.png",
    modifierGroups: [MOD_BREAD],
  },
  {
    id: "egg-salad",
    name: "Organic Egg Salad",
    description: "All-natural plant-based egg salad on sourdough",
    price: 11.99,
    category: "Sandwiches",
    image: "images/egg-salad.png",
    modifierGroups: [MOD_BREAD],
  },
  {
    id: "grilled-cheese-mushroom",
    name: "Grilled Cheese Mushroom",
    description: "Melted cheese with sautéed mushrooms",
    price: 10.99,
    category: "Sandwiches",
    image: "images/grilled-cheese-mushroom.png",
    modifierGroups: [MOD_BREAD],
  },
  {
    id: "awaken-toast",
    name: "Awaken Toast",
    description: "Seasonal specialty toast creation",
    price: 9.99,
    category: "Sandwiches",
    image: "images/avo-cucumber.png",
  },

  // ─── BITES ───
  {
    id: "onigiri",
    name: "Onigiri",
    description: "Japanese rice ball, choose your flavour",
    price: 6.99,
    category: "Bites",
    image: "images/onigiri.png",
    modifierGroups: [MOD_ONIGIRI_FLAVOUR],
  },
  {
    id: "egg-frittata",
    name: "Organic Egg Frittata",
    description: "Baked with seasonal vegetables",
    price: 6.99,
    category: "Bites",
    image: "images/egg-frittata.png",
  },
  {
    id: "chia-pudding",
    name: "Organic Chia Seed Pudding",
    description: "Made fresh daily with seasonal toppings",
    price: 6.99,
    category: "Bites",
    image: "images/egg-frittata.png",
  },
  {
    id: "jello",
    name: "5-Calorie Jello",
    description: "Light and refreshing, only 5 calories",
    price: 6.99,
    category: "Bites",
    image: "images/mochi.png",
  },
  {
    id: "elixir-shot",
    name: "Elixir Shot",
    description: "Concentrated wellness shot",
    price: 3.99,
    category: "Bites",
    image: "images/elixir-shot.png",
  },
  {
    id: "apple-chips",
    name: "Apple Chips",
    description: "Thin-sliced, oven-dried apple crisps",
    price: 2.99,
    category: "Bites",
    image: "images/apple-chips.png",
  },
  {
    id: "protein-bar",
    name: "Protein Bar",
    description: "High-protein, naturally sweetened",
    price: 2.99,
    category: "Bites",
    image: "images/protein-bar.png",
  },

  // ─── SWEETS ───
  {
    id: "brownie",
    name: "Brownie",
    description: "Rich, fudgy, house-baked",
    price: 4.99,
    category: "Sweets",
    image: "images/cookie-set.png",
  },
  {
    id: "matcha-cookie",
    name: "Matcha Cookie",
    description: "Chewy cookie with ceremonial matcha",
    price: 3.99,
    category: "Sweets",
    image: "images/cookie-set.png",
  },
  {
    id: "oat-cookie",
    name: "Organic Oat Cookie",
    description: "Wholesome oat cookie, naturally sweetened",
    price: 3.99,
    category: "Sweets",
    image: "images/cookie-set.png",
  },
  {
    id: "cranberry-almond-cookie",
    name: "Cranberry Almond Cookie",
    description: "Crunchy with cranberries and toasted almond",
    price: 3.99,
    category: "Sweets",
    image: "images/cookie-set.png",
  },
  {
    id: "apple-muffin",
    name: "Real Apple Muffin",
    description: "Made with real apple chunks, gluten-free",
    price: 4.99,
    category: "Sweets",
    image: "images/banana-muffin.png",
  },
  {
    id: "banana-muffin",
    name: "Real Banana Muffin",
    description: "Moist banana muffin, house-baked",
    price: 4.99,
    category: "Sweets",
    image: "images/banana-muffin.png",
  },
  {
    id: "cinnamon-scone",
    name: "Cinnamon Brown Sugar Scone",
    description: "Warm spiced scone with brown sugar glaze",
    price: 4.99,
    category: "Sweets",
    image: "images/sweets-set.png",
  },
  {
    id: "cheesy-scone",
    name: "High Protein Cheesy Scone",
    description: "Savory scone packed with cheese and protein",
    price: 4.99,
    category: "Sweets",
    image: "images/sweets-set.png",
  },
  {
    id: "mochi",
    name: "Strawberry Mochi",
    description: "Weekend special — fresh strawberry mochi",
    price: 4.99,
    category: "Sweets",
    image: "images/mochi.png",
  },
  {
    id: "flojo",
    name: "Flojo",
    description: "House specialty pastry",
    price: 3.99,
    category: "Sweets",
    image: "images/sweets-set.png",
  },
  {
    id: "black-sesame-spread",
    name: "Black Sesame Spread",
    description: "House-made, rich and nutty",
    price: 8.99,
    category: "Sweets",
    image: "images/cookie-set.png",
  },

  // ─── COMBOS ───
  {
    id: "sandwich-set",
    name: "Sandwich Set",
    description: "Any sandwich with your choice of drink",
    price: 17.99,
    category: "Combos",
    image: "images/sandwich-set.png",
    modifierGroups: [MOD_COMBO_SANDWICH, MOD_COMBO_DRINK],
  },
  {
    id: "onigiri-set",
    name: "Onigiri Set",
    description: "Onigiri with your choice of drink",
    price: 10.99,
    category: "Combos",
    image: "images/onigiri-set.png",
    modifierGroups: [MOD_ONIGIRI_FLAVOUR, MOD_COMBO_DRINK],
  },
  {
    id: "sweets-set",
    name: "Gluten-free Sweets Set",
    description: "Assorted gluten-free sweets with a drink",
    price: 8.99,
    category: "Combos",
    image: "images/sweets-set.png",
    modifierGroups: [MOD_COMBO_SWEET, MOD_COMBO_DRINK],
  },

  // ─── RETAIL ───
  {
    id: "gf-sourdough",
    name: "Gluten Free Sourdough Loaf",
    description: "House-baked, sourdough-style, gluten free",
    price: 13.99,
    category: "Retail",
    image: "images/cat-bread.png",
  },
  {
    id: "olive-oil",
    name: "Organic Olive Oil",
    description: "Cold-pressed, unfiltered extra virgin",
    price: 14.99,
    category: "Retail",
    image: "images/elixir-bottle.png",
  },
  {
    id: "ethiopia-harrar",
    name: "Ethiopia Harrar (150g)",
    description: "Single-origin whole bean coffee",
    price: 14.99,
    category: "Retail",
    image: "images/cat-beans.png",
  },
  {
    id: "mexico-decaf",
    name: "Mexico Decaf (150g)",
    description: "Swiss water process decaf beans",
    price: 14.99,
    category: "Retail",
    image: "images/cat-beans.png",
  },
  {
    id: "uji-matcha",
    name: "Uji Matcha (25g)",
    description: "Premium Uji ceremonial grade matcha",
    price: 30.00,
    category: "Retail",
    image: "images/cat-matcha.png",
  },
  {
    id: "house-blend",
    name: "House Blend (25g)",
    description: "Our signature matcha house blend",
    price: 25.00,
    category: "Retail",
    image: "images/cat-matcha.png",
  },
];

// Live menu — overwritten by Worker data on load, falls back to FALLBACK_MENU
let MENU = FALLBACK_MENU;

// ---- APP STATE ----
const cart = [];  // Array of { cartLineId, itemId, modifiers: { groupId: [optionId,...] }, qty }
let customerName = null;
let lastOrderTotal = 0;
let lastOrderSubtotal = 0;
let selectedTip = 0;
let lastOrderSummary = "";
let customizeItemId = null;    // item being customized
let customizeQty = 1;          // qty in customization drawer
let customizeSelections = {};   // groupId -> [optionId, ...]
let editingCartLineId = null;  // non-null when editing an existing cart line

// ---- LOCAL STORAGE KEYS ----
const STORAGE_KEY_NAME = "gleamsip_name";
const STORAGE_KEY_EMAIL = "gleamsip_email";

// ---- INITIALIZATION ----
document.addEventListener("DOMContentLoaded", async () => {
  // Try to load live menu from Cloudflare KV
  try {
    const workerUrl = typeof CONFIG !== "undefined" ? CONFIG.stripeWorkerUrl : null;
    if (workerUrl && workerUrl !== "YOUR_CLOUDFLARE_WORKER_URL_HERE") {
      const res = await fetch(workerUrl + "/menu?t=" + Date.now());
      if (res.ok) {
        const data = await res.json();
        if (Array.isArray(data) && data.length > 0) {
          // Filter out unavailable items and resolve image URLs
          MENU = data.filter((item) => item.available !== false);
          MENU.forEach((item) => {
            if (item.image && !item.image.startsWith("http") && !item.image.startsWith("images/")) {
              item.image = workerUrl + "/image/" + item.image;
            }
            // Merge modifier groups from fallback if Worker data lacks them
            if (!item.modifierGroups || item.modifierGroups.length === 0) {
              const fallback = FALLBACK_MENU.find((f) => f.id === item.id);
              if (fallback && fallback.modifierGroups) {
                item.modifierGroups = fallback.modifierGroups;
              }
            }
          });
          // Add fallback items missing from Worker (new items not yet in KV)
          // BUT skip items that exist in Worker data as unavailable
          const liveIds = new Set(MENU.map((m) => m.id));
          const unavailableIds = new Set(data.filter((item) => item.available === false).map((m) => m.id));
          FALLBACK_MENU.forEach((fb) => {
            if (!liveIds.has(fb.id) && !unavailableIds.has(fb.id)) {
              MENU.push(fb);
            }
          });
        }
      }
    }
  } catch (e) {
    console.log("Using fallback menu:", e.message);
  }

  const savedName = localStorage.getItem(STORAGE_KEY_NAME);
  if (savedName) {
    customerName = savedName;
    showApp();
  }
  setupWelcomeListeners();
  checkPaymentStatus();
});

// ---- WELCOME / LOGIN FLOW ----
function setupWelcomeListeners() {
  const continueBtn = document.getElementById("continueBtn");
  const nameInput = document.getElementById("nameInput");
  const loginBtn = document.getElementById("loginBtn");
  const loginCloseBtn = document.getElementById("loginCloseBtn");
  const signInBtn = document.getElementById("signInBtn");

  continueBtn.addEventListener("click", () => {
    const name = nameInput.value.trim();
    if (!name) {
      nameInput.focus();
      nameInput.style.borderColor = "#c75a3a";
      setTimeout(() => (nameInput.style.borderColor = ""), 1500);
      return;
    }
    customerName = name;
    localStorage.setItem(STORAGE_KEY_NAME, name);
    showApp();
  });

  nameInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") continueBtn.click();
  });

  loginBtn.addEventListener("click", () => {
    document.getElementById("loginModal").classList.add("open");
    document.getElementById("loginEmail").focus();
  });

  loginCloseBtn.addEventListener("click", () => {
    document.getElementById("loginModal").classList.remove("open");
  });

  signInBtn.addEventListener("click", () => {
    const email = document.getElementById("loginEmail").value.trim();
    const name = document.getElementById("loginName").value.trim();
    if (!email || !name) {
      if (!email) {
        document.getElementById("loginEmail").style.borderColor = "#c75a3a";
        setTimeout(() => (document.getElementById("loginEmail").style.borderColor = ""), 1500);
      }
      if (!name) {
        document.getElementById("loginName").style.borderColor = "#c75a3a";
        setTimeout(() => (document.getElementById("loginName").style.borderColor = ""), 1500);
      }
      return;
    }
    localStorage.setItem(STORAGE_KEY_NAME, name);
    localStorage.setItem(STORAGE_KEY_EMAIL, email);
    customerName = name;
    document.getElementById("loginModal").classList.remove("open");
    showApp();
  });

  document.getElementById("loginName").addEventListener("keydown", (e) => {
    if (e.key === "Enter") signInBtn.click();
  });
  document.getElementById("loginEmail").addEventListener("keydown", (e) => {
    if (e.key === "Enter") document.getElementById("loginName").focus();
  });
}

function showApp() {
  document.getElementById("welcomeScreen").classList.add("hidden");
  document.getElementById("appHeader").style.display = "";
  document.getElementById("appHero").style.display = "";
  document.getElementById("appCategoryNav").style.display = "";
  document.getElementById("menuContainer").style.display = "";
  document.getElementById("displayName").textContent = customerName;

  if (CONFIG.tagline) {
    document.getElementById("heroTagline").textContent = CONFIG.tagline;
  }

  renderCategoryNav();
  renderMenu();
  setupEventListeners();
  setupScrollSpy();
  // setupScrollReveal(); // disabled — cards visible immediately
}

// ---- CATEGORY NAVIGATION ----
function getCategories() {
  const seen = new Set();
  return MENU.map((item) => item.category).filter((cat) => {
    if (seen.has(cat)) return false;
    seen.add(cat);
    return true;
  });
}

function renderCategoryNav() {
  const nav = document.getElementById("categoryNav");
  const categories = getCategories();

  // Show group tabs (Drink, Food, Combo, Take Home) then category tabs
  const usedGroups = [];
  GROUP_ORDER.forEach(g => {
    if (categories.some(c => CATEGORY_TO_GROUP[c] === g)) usedGroups.push(g);
  });

  usedGroups.forEach((group, i) => {
    const tab = document.createElement("button");
    tab.className = "category-tab category-tab--group" + (i === 0 ? " active" : "");
    tab.textContent = group;
    tab.dataset.group = group;
    nav.appendChild(tab);
  });

  nav.addEventListener("click", (e) => {
    const tab = e.target.closest(".category-tab");
    if (!tab) return;

    nav.querySelectorAll(".category-tab").forEach((t) => t.classList.remove("active"));
    tab.classList.add("active");

    const group = tab.dataset.group;
    if (group) {
      const section = document.querySelector(`.menu-group-section[data-group="${group}"]`);
      if (section) section.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  });
}

// ---- SCROLL SPY ----
function setupScrollSpy() {
  const sections = document.querySelectorAll(".menu-group-section");
  const tabs = document.querySelectorAll(".category-tab");

  const observer = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          const group = entry.target.dataset.group;
          tabs.forEach((t) => t.classList.remove("active"));
          const activeTab = [...tabs].find((t) => t.dataset.group === group);
          if (activeTab) {
            activeTab.classList.add("active");
            const nav = activeTab.parentElement;
            const navRect = nav.getBoundingClientRect();
            const tabRect = activeTab.getBoundingClientRect();
            const scrollLeft = nav.scrollLeft + (tabRect.left - navRect.left) - (navRect.width / 2) + (tabRect.width / 2);
            nav.scrollTo({ left: scrollLeft, behavior: "smooth" });
          }
        }
      });
    },
    { rootMargin: "-120px 0px -60% 0px", threshold: 0 }
  );

  sections.forEach((section) => observer.observe(section));
}

// ---- SCROLL REVEAL ----
function setupScrollReveal() {
  const revealObserver = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          const el = entry.target;

          // Use rAF to ensure browser has painted opacity:0 before revealing
          requestAnimationFrame(() => {
            requestAnimationFrame(() => {
              // For section/group titles, reveal immediately
              if (el.classList.contains("section-title") || el.classList.contains("group-title")) {
                el.classList.add("revealed");
                return;
              }

              // For cards, stagger based on position in grid
              const card = el;
              const grid = card.parentElement;
              const cards = [...grid.querySelectorAll(".menu-card")];
              const index = cards.indexOf(card);
              const stagger = (index % 2) * 80; // 0ms for left, 80ms for right

              setTimeout(() => {
                card.classList.add("revealed");
              }, stagger);
            });
          });

          revealObserver.unobserve(el);
        }
      });
    },
    { rootMargin: "0px 0px -40px 0px", threshold: 0.05 }
  );

  document.querySelectorAll(".menu-card, .section-title, .group-title").forEach((el) => {
    revealObserver.observe(el);
  });
}

// ---- MENU RENDERING ----
function renderMenu() {
  const container = document.getElementById("menuContainer");
  const categories = getCategories();

  GROUP_ORDER.forEach((group) => {
    const groupCats = categories.filter(c => (CATEGORY_TO_GROUP[c] || "Other") === group);
    if (groupCats.length === 0) return;

    const groupSection = document.createElement("section");
    groupSection.className = "menu-group-section";
    groupSection.dataset.group = group;
    groupSection.innerHTML = `<h2 class="group-title">${group}</h2>`;
    container.appendChild(groupSection);

    groupCats.forEach((cat) => {
      const section = document.createElement("section");
      section.className = "menu-section";
      section.dataset.category = cat;

      section.innerHTML = `<h3 class="section-title">${cat}</h3>`;

      const grid = document.createElement("div");
      grid.className = "menu-grid";

      const items = MENU.filter((item) => item.category === cat);
      items.forEach((item) => {
        const card = document.createElement("div");
        card.className = "menu-card";
        card.dataset.id = item.id;
        card.innerHTML = renderMenuCard(item);
        grid.appendChild(card);
      });

      section.appendChild(grid);
      container.appendChild(section);
    });
  });
}

function renderMenuCard(item) {
  const qty = getItemTotalQty(item.id);
  const sym = CONFIG.currencySymbol;
  const imgRaw = item.image || "images/hero.png";
  const isLocal = imgRaw.startsWith("images/");
  const imgSrc = isLocal ? imgRaw.replace("images/", "images/webp/").replace(".png", ".webp") : imgRaw;
  const hasModifiers = item.modifierGroups && item.modifierGroups.length > 0;
  const priceLabel = hasModifiers ? `From ${sym}${item.price.toFixed(2)}` : `${sym}${item.price.toFixed(2)}`;

  return `
    ${qty > 0 ? `<span class="card-badge">${qty}</span>` : ""}
    <div class="card-image">
      ${isLocal ? `<picture>
        <source srcset="${imgSrc}" type="image/webp">
        <img src="${imgRaw}" alt="${item.name}" loading="lazy">
      </picture>` : `<img src="${imgSrc}" alt="${item.name}" loading="lazy">`}
    </div>
    <div class="card-body">
      <div class="card-name">${item.name}</div>
      <div class="card-desc">${item.description}</div>
      <div class="card-footer">
        <span class="card-price">${priceLabel}</span>
        ${
          hasModifiers
            ? `<button class="card-add-btn" data-action="customize" data-id="${item.id}" aria-label="Customize ${item.name}">+</button>`
            : qty === 0
              ? `<button class="card-add-btn" data-action="add" data-id="${item.id}" aria-label="Add ${item.name}">+</button>`
              : `<div class="card-qty-controls">
                  <button class="card-qty-btn" data-action="decrease" data-id="${item.id}">−</button>
                  <span class="card-qty">${qty}</span>
                  <button class="card-qty-btn" data-action="increase" data-id="${item.id}">+</button>
                </div>`
        }
      </div>
    </div>
  `;
}

function updateMenuCard(itemId) {
  const item = MENU.find((m) => m.id === itemId);
  if (!item) return;
  const el = document.querySelector(`.menu-card[data-id="${itemId}"]`);
  if (!el) return;
  el.innerHTML = renderMenuCard(item);
}

// ---- CART LOGIC ----

function generateCartLineId() {
  return "cl_" + Date.now().toString(36) + "_" + Math.random().toString(36).slice(2, 6);
}

// Total qty of a given item across all cart lines
function getItemTotalQty(itemId) {
  return cart.filter((l) => l.itemId === itemId).reduce((s, l) => s + l.qty, 0);
}

// Compute the price of a single cart line (base + modifier adjustments)
function getLinePrice(line) {
  const item = MENU.find((m) => m.id === line.itemId);
  if (!item) return 0;
  let price = item.price;
  if (item.modifierGroups && line.modifiers) {
    for (const group of item.modifierGroups) {
      const selected = line.modifiers[group.id] || [];
      for (const optId of selected) {
        const opt = group.options.find((o) => o.id === optId);
        if (opt && opt.priceAdj) price += opt.priceAdj;
      }
    }
  }
  return price;
}

// Check if two modifier selection objects are identical
function modifiersMatch(a, b) {
  const keysA = Object.keys(a || {});
  const keysB = Object.keys(b || {});
  if (keysA.length !== keysB.length) return false;
  for (const k of keysA) {
    const va = (a[k] || []).slice().sort();
    const vb = (b[k] || []).slice().sort();
    if (va.length !== vb.length || va.some((v, i) => v !== vb[i])) return false;
  }
  return true;
}

function addToCart(itemId) {
  const item = MENU.find((m) => m.id === itemId);
  if (!item) return;

  // Items with modifiers must go through customization drawer
  if (item.modifierGroups && item.modifierGroups.length > 0) {
    openCustomize(itemId);
    return;
  }

  // Simple item — find existing line or create new
  const existing = cart.find((l) => l.itemId === itemId && Object.keys(l.modifiers || {}).length === 0);
  if (existing) {
    existing.qty++;
  } else {
    cart.push({ cartLineId: generateCartLineId(), itemId, modifiers: {}, qty: 1 });
  }
  updateMenuCard(itemId);
  updateCartUI();
  showToast("Added to order");
}

// Add a customized item to cart (called from customization drawer)
function addCustomizedToCart(itemId, modifiers, qty) {
  const existing = cart.find((l) => l.itemId === itemId && modifiersMatch(l.modifiers, modifiers));
  if (existing) {
    existing.qty += qty;
  } else {
    cart.push({ cartLineId: generateCartLineId(), itemId, modifiers: { ...modifiers }, qty });
  }
  updateMenuCard(itemId);
  updateCartUI();
  showToast("Added to order");
}

function increaseLineQty(cartLineId) {
  const line = cart.find((l) => l.cartLineId === cartLineId);
  if (line) {
    line.qty++;
    updateMenuCard(line.itemId);
    updateCartUI();
  }
}

function decreaseLineQty(cartLineId) {
  const idx = cart.findIndex((l) => l.cartLineId === cartLineId);
  if (idx === -1) return;
  const line = cart[idx];
  line.qty--;
  if (line.qty <= 0) cart.splice(idx, 1);
  updateMenuCard(line.itemId);
  updateCartUI();
}

// Legacy wrappers for simple items (used by menu card +/- buttons)
function increaseQty(itemId) {
  const line = cart.find((l) => l.itemId === itemId);
  if (line) increaseLineQty(line.cartLineId);
}

function decreaseQty(itemId) {
  const line = cart.find((l) => l.itemId === itemId);
  if (line) decreaseLineQty(line.cartLineId);
}

function getCartCount() {
  return cart.reduce((sum, l) => sum + l.qty, 0);
}

function getCartSubtotal() {
  return cart.reduce((total, line) => total + getLinePrice(line) * line.qty, 0);
}

function updateCartUI() {
  const count = getCartCount();
  const badge = document.getElementById("cartBadge");
  const fab = document.getElementById("cartFab");

  badge.textContent = count;
  badge.classList.toggle("show", count > 0);
  fab.classList.toggle("hidden", count === 0);

  renderCartItems();
}

// Format modifier selections as a readable string
function formatModifierSummary(line) {
  const item = MENU.find((m) => m.id === line.itemId);
  if (!item || !item.modifierGroups || !line.modifiers) return "";
  const parts = [];
  for (const group of item.modifierGroups) {
    const selected = line.modifiers[group.id] || [];
    for (const optId of selected) {
      const opt = group.options.find((o) => o.id === optId);
      if (opt) parts.push(opt.name);
    }
  }
  return parts.join(" · ");
}

function renderCartItems() {
  const container = document.getElementById("cartItems");
  const totalsContainer = document.getElementById("cartTotals");
  const orderBtn = document.getElementById("placeOrderBtn");
  const sym = CONFIG.currencySymbol;

  if (cart.length === 0) {
    container.innerHTML = `
      <div class="cart-empty">
        <div class="cart-empty-icon">&#9749;</div>
        <p>Your order is empty</p>
      </div>
    `;
    totalsContainer.innerHTML = "";
    orderBtn.disabled = true;
    return;
  }

  container.innerHTML = cart
    .map((line) => {
      const item = MENU.find((m) => m.id === line.itemId);
      if (!item) return "";
      const linePrice = getLinePrice(line);
      const modSummary = formatModifierSummary(line);
      const hasModifiers = item.modifierGroups && item.modifierGroups.length > 0;
      return `
        <div class="cart-item" ${hasModifiers ? `data-action="edit-line" data-line="${line.cartLineId}"` : ""} style="${hasModifiers ? "cursor:pointer" : ""}">
          <div class="cart-item-info">
            <div class="cart-item-name">${item.name}</div>
            ${modSummary ? `<div class="cart-item-mods">${modSummary}</div>` : ""}
            <div class="cart-item-price">${sym}${linePrice.toFixed(2)} each</div>
          </div>
          <div class="cart-item-controls">
            <button class="qty-btn" data-action="line-decrease" data-line="${line.cartLineId}">−</button>
            <span class="qty-value">${line.qty}</span>
            <button class="qty-btn" data-action="line-increase" data-line="${line.cartLineId}">+</button>
          </div>
          <div class="cart-item-total">${sym}${(linePrice * line.qty).toFixed(2)}</div>
        </div>
      `;
    })
    .join("");

  const subtotal = getCartSubtotal();
  const tax = subtotal * CONFIG.taxRate;
  const total = subtotal + tax;

  totalsContainer.innerHTML = `
    <div class="cart-total-row"><span>Subtotal</span><span>${sym}${subtotal.toFixed(2)}</span></div>
    <div class="cart-total-row"><span>Tax</span><span>${sym}${tax.toFixed(2)}</span></div>
    <div class="cart-total-row total"><span>Total</span><span>${sym}${total.toFixed(2)}</span></div>
  `;

  orderBtn.disabled = false;
}

// ---- CART DRAWER TOGGLE ----
function openCart() {
  document.getElementById("cartOverlay").classList.add("open");
  document.getElementById("cartDrawer").classList.add("open");
  document.body.style.overflow = "hidden";
  renderCartItems();
}

function closeCart() {
  document.getElementById("cartOverlay").classList.remove("open");
  document.getElementById("cartDrawer").classList.remove("open");
  document.body.style.overflow = "";
}

function toggleCart() {
  const drawer = document.getElementById("cartDrawer");
  if (drawer.classList.contains("open")) closeCart();
  else openCart();
}

// ---- PLACE ORDER ----
async function placeOrder() {
  const orderBtn = document.getElementById("placeOrderBtn");
  if (cart.length === 0) return;

  orderBtn.disabled = true;
  orderBtn.innerHTML = '<span class="spinner"></span> Placing order...';

  const subtotal = getCartSubtotal();
  const tax = subtotal * CONFIG.taxRate;
  const total = subtotal + tax;

  const orderItems = cart.map((line) => {
    const item = MENU.find((m) => m.id === line.itemId);
    const linePrice = getLinePrice(line);
    const modSummary = formatModifierSummary(line);

    // Build modifier array for kitchen display / receipts
    const modDetails = [];
    if (item.modifierGroups && line.modifiers) {
      for (const group of item.modifierGroups) {
        const selected = line.modifiers[group.id] || [];
        for (const optId of selected) {
          const opt = group.options.find((o) => o.id === optId);
          if (opt) modDetails.push({ group: group.name, option: opt.name, priceAdj: opt.priceAdj || 0 });
        }
      }
    }

    return {
      name: item.name,
      qty: line.qty,
      price: linePrice,
      basePrice: item.price,
      total: linePrice * line.qty,
      modifiers: modDetails,
      modifierSummary: modSummary,
    };
  });

  await sendOrderToWorker(orderItems, subtotal, tax, total);

  closeCart();
  lastOrderSubtotal = subtotal;
  lastOrderSummary = orderItems.map((i) => {
    const modStr = i.modifierSummary ? ` (${i.modifierSummary})` : "";
    return `${i.qty}x ${i.name}${modStr}`;
  }).join(", ");
  showConfirmation(total);

  // Clear cart
  cart.length = 0;
  updateCartUI();

  document.querySelectorAll(".menu-card").forEach((el) => {
    const id = el.dataset.id;
    const item = MENU.find((m) => m.id === id);
    if (item) el.innerHTML = renderMenuCard(item);
  });

  orderBtn.innerHTML = "Place Order";
  orderBtn.disabled = true;
}

// ---- CONFIRMATION MODAL ----
function showConfirmation(total) {
  const modal = document.getElementById("confirmationModal");
  document.getElementById("confirmName").textContent = customerName;
  modal.classList.add("open");

  // Build tip options
  selectedTip = 0;
  lastOrderTotal = total;
  buildTipOptions(lastOrderSubtotal);

  // Update pay button amount
  updatePayButton();
}

function buildTipOptions(subtotal) {
  const container = document.getElementById("tipOptions");
  const sym = CONFIG.currencySymbol;
  const fifteenPct = subtotal * 0.15;

  let options;
  if (fifteenPct < 1) {
    // Small order — show flat dollar amounts
    options = [
      { label: `${sym}1`, value: 1 },
      { label: `${sym}2`, value: 2 },
      { label: `${sym}3`, value: 3 },
    ];
  } else {
    // Normal order — show percentages
    options = [
      { label: "15%", value: Math.round(subtotal * 0.15 * 100) / 100 },
      { label: "18%", value: Math.round(subtotal * 0.18 * 100) / 100 },
      { label: "20%", value: Math.round(subtotal * 0.20 * 100) / 100 },
      { label: "25%", value: Math.round(subtotal * 0.25 * 100) / 100 },
    ];
  }

  container.innerHTML = options
    .map((opt) => `<button class="tip-btn" data-tip="${opt.value}">${opt.label}</button>`)
    .join("") +
    `<button class="tip-btn no-tip active" data-tip="0">No tip</button>`;

  // Tip button click handlers
  container.querySelectorAll(".tip-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      container.querySelectorAll(".tip-btn").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      selectedTip = parseFloat(btn.dataset.tip);
      updateTipTotal();
    });
  });

  updateTipTotal();
}

function updateTipTotal() {
  const el = document.getElementById("tipTotal");
  const sym = CONFIG.currencySymbol;
  const grandTotal = lastOrderTotal + selectedTip;

  if (selectedTip > 0) {
    el.innerHTML = `Tip: <span class="tip-amount">${sym}${selectedTip.toFixed(2)}</span>`;
  } else {
    el.innerHTML = `Total: <span class="tip-amount">${sym}${lastOrderTotal.toFixed(2)}</span>`;
  }

  updatePayButton();
}

function updatePayButton() {
  const textEl = document.getElementById("stripePayText");
  if (!textEl) return;

  const sym = CONFIG.currencySymbol;
  const grandTotal = lastOrderTotal + selectedTip;
  textEl.textContent = `Pay ${sym}${grandTotal.toFixed(2)}`;
}

// ---- STRIPE CHECKOUT ----
async function startStripeCheckout() {
  const payBtn = document.getElementById("stripePayBtn");
  const textEl = document.getElementById("stripePayText");
  const spinnerEl = document.getElementById("stripePaySpinner");
  const workerUrl = CONFIG.stripeWorkerUrl;

  // If Stripe not configured, fall back to pay-at-counter message
  if (!workerUrl || workerUrl === "YOUR_CLOUDFLARE_WORKER_URL_HERE") {
    showToast("Online payment coming soon! Please pay at counter.");
    return;
  }

  // Show loading state
  textEl.classList.add("hidden");
  spinnerEl.classList.remove("hidden");
  payBtn.disabled = true;

  const sym = CONFIG.currencySymbol;
  const grandTotal = lastOrderTotal + selectedTip;

  // Build success/cancel URLs
  const baseUrl = window.location.origin + window.location.pathname;
  const successUrl = `${baseUrl}?payment=success`;
  const cancelUrl = `${baseUrl}?payment=cancelled`;

  try {
    const response = await fetch(workerUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        amount: grandTotal,
        customerName: customerName,
        orderSummary: lastOrderSummary,
        successUrl: successUrl,
        cancelUrl: cancelUrl,
      }),
    });

    const data = await response.json();

    if (data.url) {
      // Redirect to Stripe Checkout
      window.location.href = data.url;
    } else {
      throw new Error(data.error || "Could not create checkout session");
    }
  } catch (err) {
    console.error("Stripe checkout error:", err);
    showToast("Payment error — please pay at counter");
    // Reset button
    textEl.classList.remove("hidden");
    spinnerEl.classList.add("hidden");
    payBtn.disabled = false;
  }
}

// ---- ORDER WORKER POST (Kitchen Display) ----
// Posts the order to the Cloudflare Worker so it appears on the kitchen display.
async function sendOrderToWorker(items, subtotal, tax, total) {
  const workerUrl = typeof CONFIG !== "undefined" ? CONFIG.stripeWorkerUrl : null;
  if (!workerUrl || workerUrl === "YOUR_CLOUDFLARE_WORKER_URL_HERE") return;

  try {
    const res = await fetch(workerUrl + "/api/orders", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        customerName,
        items: items.map((i) => ({
          name: i.name,
          qty: i.qty,
          price: i.price,
          basePrice: i.basePrice,
          modifiers: i.modifiers,
        })),
        subtotal,
        tax,
        total,
      }),
    });
    const data = await res.json();
    if (data.ok && data.order && data.order.id) {
      // Save order ID so we can update payment status after Stripe redirect
      localStorage.setItem("gleamsip_last_order_id", data.order.id);
    }
  } catch (err) {
    console.log("Worker order post failed:", err.message);
  }
}

// Update order payment status after Stripe redirect
async function updateOrderPayment(status) {
  const workerUrl = typeof CONFIG !== "undefined" ? CONFIG.stripeWorkerUrl : null;
  const orderId = localStorage.getItem("gleamsip_last_order_id");
  if (!workerUrl || !orderId) return;

  try {
    await fetch(workerUrl + "/api/orders/" + orderId, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ paymentStatus: status, paymentMethod: "stripe" }),
    });
    localStorage.removeItem("gleamsip_last_order_id");
  } catch (err) {
    console.log("Order payment update failed:", err.message);
  }
}

// ---- PAYMENT SUCCESS/CANCEL HANDLING ----
function checkPaymentStatus() {
  const params = new URLSearchParams(window.location.search);
  const status = params.get("payment");

  if (status === "success") {
    window.history.replaceState({}, "", window.location.pathname);
    // Update order with payment status
    updateOrderPayment("paid");
    // Show full-screen payment success modal
    const name = customerName || localStorage.getItem(STORAGE_KEY_NAME) || "Guest";
    document.getElementById("paidName").textContent = name;
    document.getElementById("paymentSuccessModal").classList.add("open");
    document.getElementById("paidDoneBtn").addEventListener("click", () => {
      document.getElementById("paymentSuccessModal").classList.remove("open");
    });
  } else if (status === "cancelled") {
    window.history.replaceState({}, "", window.location.pathname);
    updateOrderPayment("cancelled");
    setTimeout(() => showToast("Payment cancelled"), 500);
  }
}

function closeConfirmation() {
  document.getElementById("confirmationModal").classList.remove("open");
  selectedTip = 0;
}

// ---- TOAST NOTIFICATION ----
let toastTimeout;
function showToast(message) {
  const toast = document.getElementById("toastNotification");
  toast.textContent = message;
  toast.classList.add("show");
  clearTimeout(toastTimeout);
  toastTimeout = setTimeout(() => toast.classList.remove("show"), 1800);
}

// ---- EVENT LISTENERS ----
function setupEventListeners() {
  document.getElementById("cartFab").addEventListener("click", toggleCart);
  document.getElementById("cartOverlay").addEventListener("click", closeCart);
  document.getElementById("cartCloseBtn").addEventListener("click", closeCart);
  document.getElementById("placeOrderBtn").addEventListener("click", placeOrder);
  document.getElementById("orderMoreBtn").addEventListener("click", closeConfirmation);
  document.getElementById("stripePayBtn").addEventListener("click", startStripeCheckout);
  document.getElementById("payCounterBtn").addEventListener("click", () => {
    closeConfirmation();
    showToast("Please pay at the counter — thank you!");
  });

  document.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-action]");
    if (!btn) return;

    const action = btn.dataset.action;
    const id = btn.dataset.id;
    const lineId = btn.dataset.line;

    switch (action) {
      case "add": addToCart(id); break;
      case "customize": openCustomize(id); break;
      case "increase": increaseQty(id); break;
      case "decrease": decreaseQty(id); break;
      case "line-increase": increaseLineQty(lineId); renderCartItems(); break;
      case "line-decrease": decreaseLineQty(lineId); renderCartItems(); break;
      case "edit-line": openCustomizeForEdit(lineId); break;
    }
  });
}

// ---- CUSTOMIZATION DRAWER ----

function openCustomize(itemId, existingSelections, existingQty) {
  const item = MENU.find((m) => m.id === itemId);
  if (!item) return;

  customizeItemId = itemId;
  customizeQty = existingQty || 1;
  editingCartLineId = null;

  // Initialize selections — either from existing or defaults
  if (existingSelections) {
    customizeSelections = JSON.parse(JSON.stringify(existingSelections));
  } else {
    customizeSelections = {};
    // Auto-select first option for required single-select groups
    if (item.modifierGroups) {
      for (const group of item.modifierGroups) {
        if (group.required && group.maxSelect === 1 && group.options.length > 0) {
          customizeSelections[group.id] = [group.options[0].id];
        }
      }
    }
  }

  renderCustomizeDrawer(item);

  document.getElementById("customizeOverlay").classList.add("open");
  document.getElementById("customizeDrawer").classList.add("open");
  document.body.style.overflow = "hidden";
}

function openCustomizeForEdit(cartLineId) {
  const line = cart.find((l) => l.cartLineId === cartLineId);
  if (!line) return;
  editingCartLineId = cartLineId;
  openCustomize(line.itemId, line.modifiers, line.qty);
}

function closeCustomize() {
  document.getElementById("customizeOverlay").classList.remove("open");
  document.getElementById("customizeDrawer").classList.remove("open");
  document.body.style.overflow = "";
  customizeItemId = null;
  editingCartLineId = null;
}

function renderCustomizeDrawer(item) {
  const sym = CONFIG.currencySymbol;
  const imgRaw = item.image || "images/hero.png";
  const imgSrc = imgRaw.startsWith("images/") ? imgRaw.replace("images/", "images/webp/").replace(".png", ".webp") : imgRaw;

  document.getElementById("customizeImg").src = imgSrc;
  document.getElementById("customizeName").textContent = item.name;
  document.getElementById("customizeDesc").textContent = item.description || "";
  document.getElementById("customizeQty").textContent = customizeQty;

  // Render modifier groups
  const body = document.getElementById("customizeBody");
  if (!item.modifierGroups || item.modifierGroups.length === 0) {
    body.innerHTML = "";
  } else {
    body.innerHTML = item.modifierGroups.map((group) => {
      const selected = customizeSelections[group.id] || [];
      const isSingle = group.maxSelect === 1;

      const optionsHtml = group.options.map((opt) => {
        const isSelected = selected.includes(opt.id);
        const priceStr = opt.priceAdj > 0 ? `<span class="mod-option-price">+${sym}${opt.priceAdj.toFixed(2)}</span>` : "";
        return `<button class="mod-option ${isSelected ? "selected" : ""}"
          data-group="${group.id}" data-option="${opt.id}" data-single="${isSingle}">
          ${opt.name} ${priceStr}
        </button>`;
      }).join("");

      return `
        <div class="mod-group">
          <div class="mod-group-header">
            <span class="mod-group-name">${group.name}</span>
            ${group.required ? '<span class="mod-required-badge">Required</span>' : ""}
          </div>
          <div class="mod-options">${optionsHtml}</div>
        </div>
      `;
    }).join("");
  }

  updateCustomizeButton();
}

function updateCustomizeButton() {
  const item = MENU.find((m) => m.id === customizeItemId);
  if (!item) return;
  const sym = CONFIG.currencySymbol;

  // Check if all required groups are satisfied
  let allRequiredMet = true;
  if (item.modifierGroups) {
    for (const group of item.modifierGroups) {
      if (group.required) {
        const selected = customizeSelections[group.id] || [];
        if (selected.length === 0) { allRequiredMet = false; break; }
      }
    }
  }

  // Calculate price
  const fakeLine = { itemId: item.id, modifiers: customizeSelections, qty: customizeQty };
  const unitPrice = getLinePrice(fakeLine);
  const totalPrice = unitPrice * customizeQty;

  const btn = document.getElementById("customizeAddBtn");
  const label = editingCartLineId ? "Update Order" : "Add to Order";
  btn.textContent = `${label} — ${sym}${totalPrice.toFixed(2)}`;
  btn.disabled = !allRequiredMet;
}

function handleModifierOptionClick(groupId, optionId, isSingle) {
  const item = MENU.find((m) => m.id === customizeItemId);
  if (!item) return;
  const group = item.modifierGroups.find((g) => g.id === groupId);
  if (!group) return;

  let selected = customizeSelections[groupId] || [];

  if (isSingle) {
    // Radio behavior — always select the clicked one
    selected = [optionId];
  } else {
    // Toggle behavior for multi-select
    const idx = selected.indexOf(optionId);
    if (idx >= 0) {
      selected.splice(idx, 1);
    } else if (selected.length < group.maxSelect) {
      selected.push(optionId);
    }
  }

  customizeSelections[groupId] = selected;
  renderCustomizeDrawer(item);
}

function confirmCustomize() {
  const item = MENU.find((m) => m.id === customizeItemId);
  if (!item) return;

  if (editingCartLineId) {
    // Update existing cart line
    const line = cart.find((l) => l.cartLineId === editingCartLineId);
    if (line) {
      line.modifiers = { ...customizeSelections };
      line.qty = customizeQty;
    }
    updateMenuCard(item.id);
    updateCartUI();
    renderCartItems();
    showToast("Updated");
  } else {
    addCustomizedToCart(item.id, customizeSelections, customizeQty);
  }
  closeCustomize();
}

// ---- CUSTOMIZATION EVENT LISTENERS (set up once) ----
document.addEventListener("DOMContentLoaded", () => {
  document.getElementById("customizeOverlay").addEventListener("click", closeCustomize);
  document.getElementById("customizeCloseBtn").addEventListener("click", closeCustomize);

  document.getElementById("customizeAddBtn").addEventListener("click", confirmCustomize);

  document.getElementById("customizeQtyPlus").addEventListener("click", () => {
    customizeQty++;
    document.getElementById("customizeQty").textContent = customizeQty;
    updateCustomizeButton();
  });

  document.getElementById("customizeQtyMinus").addEventListener("click", () => {
    if (customizeQty > 1) {
      customizeQty--;
      document.getElementById("customizeQty").textContent = customizeQty;
      updateCustomizeButton();
    }
  });

  // Delegate clicks on modifier option buttons
  document.getElementById("customizeBody").addEventListener("click", (e) => {
    const btn = e.target.closest(".mod-option");
    if (!btn) return;
    handleModifierOptionClick(btn.dataset.group, btn.dataset.option, btn.dataset.single === "true");
  });
});
