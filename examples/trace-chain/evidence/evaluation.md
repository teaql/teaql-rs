<!-- ephemeral -->
# KSML Evaluation Report
## 📊 Summary
- **Errors**: 0
- **Warnings**: 0
- **Suggestions**: 0
- **Solids**: 17

## ✅ Solids

| Rule ID | Target | Source | Message |
|---------|--------|--------|---------|
| `KSML-UPLOAD-001` | `` | — | The upload contains 1 files. |
| `KSML-UPLOAD-005` | `` | `model.xml:2` | Resolved entrypoint: model.xml |
| `KSML-XML-002` | `` | `model.xml:2` | The XML document model.xml was parsed successfully. |
| `KSML-ROOT-003` | `` | `model.xml:2` | Root name 'trace-chain-service' is well-formed. |
| `KSML-ROOT-008` | `` | `model.xml:2` | External JSON business fields use the default camelCase profile. |
| `KSML-OBJECT-001` | `platform` | `model.xml:4` | Object 'platform' defines display name, module, and module key metadata. |
| `KSML-OBJECT-001` | `customer_order` | `model.xml:6` | Object 'customer_order' defines display name, module, and module key metadata. |
| `KSML-OBJECT-001` | `order_item` | `model.xml:9` | Object 'order_item' defines display name, module, and module key metadata. |
| `KSML-OBJECT-001` | `payment` | `model.xml:12` | Object 'payment' defines display name, module, and module key metadata. |
| `KSML-OBJECT-001` | `payment_attempt` | `model.xml:14` | Object 'payment_attempt' defines display name, module, and module key metadata. |
| `KSML-OBJECT-001` | `shipment` | `model.xml:16` | Object 'shipment' defines display name, module, and module key metadata. |
| `KSML-REFERENCE-003` | `customer_order.platform` | `model.xml:7` | Reference 'platform' in 'customer_order' successfully resolves to target object 'platform'. |
| `KSML-REFERENCE-003` | `order_item.customer_order` | `model.xml:11` | Reference 'customer_order' in 'order_item' successfully resolves to target object 'customer_order'. |
| `KSML-REFERENCE-003` | `payment.customer_order` | `model.xml:13` | Reference 'customer_order' in 'payment' successfully resolves to target object 'customer_order'. |
| `KSML-REFERENCE-003` | `payment_attempt.payment` | `model.xml:15` | Reference 'payment' in 'payment_attempt' successfully resolves to target object 'payment'. |
| `KSML-REFERENCE-003` | `shipment.customer_order` | `model.xml:17` | Reference 'customer_order' in 'shipment' successfully resolves to target object 'customer_order'. |
| `KSML-DOMAIN-ROOT-003` | `` | `model.xml:2` | Exactly one domain root candidate 'platform' was found. |

