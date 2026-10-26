export const DEFAULT_STORAGE_INVOICE_TEMPLATE = {
  templateId: 'storage-invoice',
  category: 'Storage invoice',
  subject: {
    en: 'Storage invoice {{invoice_id}} - {{saison}}',
    fr: 'Facture d’entreposage {{invoice_id}} - {{saison}}'
  },
  body: {
    en: 'Hello {{tenant}},\n\nYour vehicle(s) are now stored for the {{saison}} season. Please find your invoice below and its PDF copy attached.\n\n{{invoice_details}}\n\nThe balance due is {{balance_due}}. Please send your payment by e-Transfer to warehouse@as-colle.com.\n\nThank you,\nThe Ferme Colle team.',
    fr: 'Bonjour {{tenant}},\n\nVotre(vos) véhicule(s) sont maintenant entreposés pour la saison « {{saison}} ». Vous trouverez votre facture ci-dessous ainsi que sa version PDF en pièce jointe.\n\n{{invoice_details}}\n\nLe solde à payer est de {{balance_due}}. Veuillez effectuer votre paiement par virement Interac à entrepot@as-colle.com.\n\nMerci,\nL’équipe de la Ferme Colle.'
  }
};

export const DEFAULT_STORAGE_INVOICE_REMINDER_TEMPLATE = {
  templateId: 'storage-invoice-reminder', category: 'Storage invoice reminder',
  subject: { en: 'Friendly reminder - invoice {{invoice_id}} - {{saison}}', fr: 'Petit rappel amical - facture {{invoice_id}} - {{saison}}' },
  body: {
    en: 'Hello {{tenant}},\n\nThis is a friendly reminder that our records show an outstanding balance of {{balance_due}} on your storage invoice {{invoice_id}} for {{saison}}. A copy of your original invoice is attached for your convenience.\n\n{{invoice_details}}\n\nWhen you have a moment, please send your payment by e-Transfer to warehouse@as-colle.com. If you have already paid, thank you! Please disregard this reminder and let us know so we can update our records. If you have any questions, simply reply to this email.\n\nThank you,\nThe Ferme Colle team.',
    fr: 'Bonjour {{tenant}},\n\nUn petit rappel amical : selon nos dossiers, un solde de {{balance_due}} reste à payer sur votre facture d’entreposage {{invoice_id}} pour la saison « {{saison}} ». Une copie de votre facture originale est jointe pour votre commodité.\n\n{{invoice_details}}\n\nLorsque vous aurez un moment, merci d’effectuer votre paiement par virement Interac à entrepot@as-colle.com. Si vous avez déjà payé, merci ! Veuillez ignorer ce rappel et nous en informer afin que nous puissions mettre nos dossiers à jour. Pour toute question, il suffit de répondre à ce courriel.\n\nMerci,\nL’équipe de la Ferme Colle.'
  }
};

export const DEFAULT_STORAGE_PAID_TEMPLATE = {
  templateId: 'storage-paid', category: 'Storage payment receipt',
  subject: { en: 'Payment received - invoice {{invoice_id}} - {{saison}}', fr: 'Paiement reçu - facture {{invoice_id}} - {{saison}}' },
  body: {
    en: 'Hello {{tenant}},\n\nWe confirm receipt of your payment of {{payment_amount}} on {{paid_date}}. Your storage invoice {{invoice_id}} is now paid in full.\n\nYour invoice marked PAID is attached as a PDF. The remaining balance is {{balance_due}}.\n\n{{invoice_details}}\n\nThank you,\nThe Ferme Colle team.',
    fr: 'Bonjour {{tenant}},\n\nNous confirmons la réception de votre paiement de {{payment_amount}} le {{paid_date}}. Votre facture d’entreposage {{invoice_id}} est maintenant payée en totalité.\n\nVotre facture portant le sceau PAYÉ est jointe en format PDF. Le solde restant est de {{balance_due}}.\n\n{{invoice_details}}\n\nMerci,\nL’équipe de la Ferme Colle.'
  }
};
