// Real, public geographic reference data — actual cities and countries,
// not fabricated ones. Chosen deliberately over a synthetic list: cities
// and countries are geographic facts with no privacy/reputation stakes
// (unlike using real people's or real companies' names as filler for
// invented "customer activity" records — see README "Where AI deviated
// from spec" / the conversation that led to this file).
//
// `all-the-cities` ships MaxMind/GeoNames-derived public city data;
// `iso-3166-1` ships the standard ISO country-code table. Both are
// reference datasets, not scraped personal records.
const cities = require('all-the-cities');
const iso = require('iso-3166-1');

// Population floor keeps the pool to cities someone would recognize
// (avoids a 2M-row demo full of villages), while still giving hundreds of
// distinct, real (city, country) pairs — far more variety than a
// hand-picked list of ~8 cities could.
const MIN_POPULATION = 300000;

const CITY_COUNTRY_PAIRS = cities
  .filter((c) => c.population >= MIN_POPULATION)
  .map((c) => {
    const country = iso.whereAlpha2(c.country);
    return country ? { city: c.name, country: country.country } : null;
  })
  .filter(Boolean);

module.exports = { CITY_COUNTRY_PAIRS };
