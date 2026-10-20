export function readSkipEmail(data) {
  if (data?.skipEmail != null && typeof data.skipEmail !== 'boolean') throw Object.assign(new Error('skipEmail must be a boolean.'), { code: 'invalid-argument' });
  return data?.skipEmail === true;
}

export function workflowEmailAudit(prefix, { skipEmail, timestamp, uid, delivery }) {
  if (skipEmail) return {
    [`${prefix}EmailDeliveryStatus`]: 'skipped',
    [`${prefix}EmailSkippedAt`]: timestamp, [`${prefix}EmailSkippedBy`]: uid
  };
  return {
    [`${prefix}EmailDeliveryStatus`]: 'accepted',
    [`${prefix}EmailSentAt`]: timestamp, [`${prefix}EmailSentBy`]: uid,
    [`${prefix}EmailSentTo`]: delivery.to, [`${prefix}EmailCc`]: delivery.cc,
    [`${prefix}EmailDevRedirect`]: delivery.developmentEmailRedirect
  };
}
