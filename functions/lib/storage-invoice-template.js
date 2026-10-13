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

export const DEFAULT_STORAGE_PAID_TEMPLATE = {
  templateId: 'storage-paid', category: 'Storage payment receipt',
  subject: { en: 'Payment received - invoice {{invoice_id}} - {{saison}}', fr: 'Paiement reçu - facture {{invoice_id}} - {{saison}}' },
  body: {
    en: 'Hello {{tenant}},\n\nWe confirm receipt of your payment of {{payment_amount}} on {{paid_date}}. Your storage invoice {{invoice_id}} is now paid in full.\n\nYour invoice marked PAID is attached as a PDF. The remaining balance is {{balance_due}}.\n\n{{invoice_details}}\n\nThank you,\nThe Ferme Colle team.',
    fr: 'Bonjour {{tenant}},\n\nNous confirmons la réception de votre paiement de {{payment_amount}} le {{paid_date}}. Votre facture d’entreposage {{invoice_id}} est maintenant payée en totalité.\n\nVotre facture portant le sceau PAYÉ est jointe en format PDF. Le solde restant est de {{balance_due}}.\n\n{{invoice_details}}\n\nMerci,\nL’équipe de la Ferme Colle.'
  }
};
