# TeaQL S3 object storage provider

Implements the runtime ObjectStorageProvider trait using the official AWS SDK.
The provider is optional; core contracts have no AWS dependency. Configure a
shared client at startup and access it through UserContext with a comment.
See the [runtime example](../examples/object-storage/README.md).

Errors are sanitized categories, never raw SDK responses. HEAD 403/5xx remain
errors; an object HEAD 404 confirms bucket access before becoming absence.
No bucket creation, domain permissions or SQL transaction is implied.
