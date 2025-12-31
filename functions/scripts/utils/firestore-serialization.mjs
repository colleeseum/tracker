import admin from 'firebase-admin';

const Timestamp = admin.firestore.Timestamp;
const GeoPoint = admin.firestore.GeoPoint;
const BlobClass = admin.firestore.Blob;

function isDocumentReference(value) {
  return (
    value &&
    typeof value === 'object' &&
    typeof value.path === 'string' &&
    typeof value.id === 'string' &&
    typeof value.firestore === 'object'
  );
}

function isPlainObject(value) {
  return Object.prototype.toString.call(value) === '[object Object]';
}

function serializeValue(value) {
  if (value === null || value === undefined) {
    return value ?? null;
  }
  if (Timestamp && value instanceof Timestamp) {
    return { __datatype: 'timestamp', value: value.toDate().toISOString() };
  }
  if (GeoPoint && value instanceof GeoPoint) {
    return {
      __datatype: 'geopoint',
      latitude: value.latitude,
      longitude: value.longitude
    };
  }
  if (isDocumentReference(value)) {
    return {
      __datatype: 'documentReference',
      path: value.path
    };
  }
  if (BlobClass && value instanceof BlobClass) {
    return {
      __datatype: 'blob',
      base64: value.toBase64()
    };
  }
  if (Array.isArray(value)) {
    return value.map((entry) => serializeValue(entry));
  }
  if (isPlainObject(value)) {
    const output = {};
    Object.entries(value).forEach(([key, entry]) => {
      output[key] = serializeValue(entry);
    });
    return output;
  }
  return value;
}

function deserializeValue(value, db) {
  if (value === null || value === undefined) {
    return value ?? null;
  }
  if (Array.isArray(value)) {
    return value.map((entry) => deserializeValue(entry, db));
  }
  if (!isPlainObject(value)) {
    return value;
  }
  switch (value.__datatype) {
    case 'timestamp':
      return Timestamp.fromDate(new Date(value.value));
    case 'geopoint':
      return new GeoPoint(Number(value.latitude) || 0, Number(value.longitude) || 0);
    case 'documentReference':
      if (!db) {
        throw new Error('Cannot deserialize DocumentReference without Firestore instance.');
      }
      return db.doc(value.path);
    case 'blob':
      if (!BlobClass || typeof BlobClass.fromBase64String !== 'function') {
        throw new Error('Blob support is not available in the current firebase-admin version.');
      }
      return BlobClass.fromBase64String(value.base64 || '');
    default: {
      const output = {};
      Object.entries(value).forEach(([key, entry]) => {
        output[key] = deserializeValue(entry, db);
      });
      return output;
    }
  }
}

export function serializeData(data) {
  if (!isPlainObject(data)) {
    return serializeValue(data);
  }
  const result = {};
  Object.entries(data).forEach(([key, value]) => {
    result[key] = serializeValue(value);
  });
  return result;
}

export function deserializeData(data, db) {
  if (!isPlainObject(data)) {
    return deserializeValue(data, db);
  }
  const result = {};
  Object.entries(data).forEach(([key, value]) => {
    result[key] = deserializeValue(value, db);
  });
  return result;
}
