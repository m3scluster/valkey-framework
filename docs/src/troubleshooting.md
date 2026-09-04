# Troubleshooting

This section provides guidance for common issues and error scenarios when working with the Valkey Mesos Control Plane.

## Common Issues

### 1. Missing Environment Variables

**Problem**: Application fails to start due to missing configuration  
**Solution**: Ensure all required environment variables are set:
```bash
MESOS_MASTER=mesos.example.test:5050 \
MESOS_USERNAME=example-user \
MESOS_PASSWORD="$MESOS_PASSWORD" \
DRY_RUN=true \
make run
```

### 2. Dry Run Mode Limitations

**Problem**: Scaling operations fail in dry-run mode  
**Solution**: Understand that dry-run mode prevents actual deployment. For production behavior, set `DRY_RUN=false`, though note that full Mesos integration is currently disabled.

### 3. Port Conflicts  

**Problem**: Services fail to start due to port binding issues
**Solution**: Check for existing processes using ports 8080 and 5173:
```bash
lsof -i :8080
lsof -i :5173
```

### 4. Mesos Connection Issues  

**Problem**: Backend fails to connect to Mesos master
**Solution**: Verify that `MESOS_MASTER` address is correct and accessible from the host

## Error Messages

### `unknown_no_valkey_endpoint`  
This message appears in dry-run mode because no actual Valkey processes are running. This is expected behavior.

### Scaling Failures in Dry Run Mode  
When attempting to scale in dry-run mode, the system will present an explanatory error message instead of silently succeeding.

## Debugging Tips

1. **Check logs**: Examine console output for detailed error messages
2. **Use verbose mode**: Enable debug logging for more detailed information  
3. **Test locally first**: Use `DRY_RUN=true` to validate configurations before production deployment
4. **Verify environment variables**: Ensure all required variables are properly set

## Development Environment Setup

If you encounter issues during development, try:
1. Running `make clean` to clear build artifacts
2. Rebuilding with `make build` 
3. Verifying your Nix environment is set up correctly

## Production Deployment Considerations

When moving from development to production:
- Test configuration thoroughly in dry-run mode first
- Ensure all Mesos credentials are properly configured  
- Validate that the target Mesos cluster responds to framework registration requests