export const PROFILE_CONFIG = {
  "activeProfile": "local",
  "profiles": {
    "local": {
      "label": "Local emulator",
      "projectId": "expense-tracker-local",
      "useEmulators": true,
      "firebase": {
        "apiKey": "local",
        "authDomain": "expense-tracker-local",
        "projectId": "expense-tracker-local",
        "storageBucket": "expense-tracker-local",
        "messagingSenderId": "0",
        "appId": "tracker-local",
        "measurementId": "G-LOCAL"
      },
      "emulator": {
        "host": "127.0.0.1",
        "authPort": 9099,
        "firestorePort": 8080,
        "functionsPort": 5001
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
        "firestorePort": 8080,
        "functionsPort": 5001
      }
    }
  }
};
