export const PROFILE_CONFIG = {
  "activeProfile": "local",
  "profiles": {
    "local": {
      "label": "Local emulator",
      "projectId": "tracker-187c5",
      "useEmulators": true,
      "firebase": {
        "apiKey": "local",
        "authDomain": "tracker-187c5.firebaseapp.com",
        "projectId": "tracker-187c5",
        "storageBucket": "tracker-187c5.firebasestorage.app",
        "messagingSenderId": "0",
        "appId": "tracker-local",
        "measurementId": "G-LOCAL"
      },
      "emulator": {
        "host": "127.0.0.1",
        "authPort": 9099,
        "firestorePort": 8081,
        "functionsPort": 5001,
        "storagePort": 9199
      }
    },
    "prod": {
      "label": "Production",
      "projectId": "tracker-187c5",
      "useEmulators": false,
      "firebase": {
        "apiKey": "AIzaSyAEtdh7DvpbC4T4HaQ646alWA1T9iSfz3o",
        "authDomain": "tracker-187c5.firebaseapp.com",
        "projectId": "tracker-187c5",
        "storageBucket": "tracker-187c5.firebasestorage.app",
        "messagingSenderId": "1044638579272",
        "appId": "1:1044638579272:web:cc555fd460b3783b70f67d",
        "measurementId": "G-EB48QYSZ9Z"
      },
      "emulator": {
        "host": "127.0.0.1",
        "authPort": 9099,
        "firestorePort": 8081,
        "functionsPort": 5001,
        "storagePort": 9199
      }
    }
  }
};
