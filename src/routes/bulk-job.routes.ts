import { Router } from 'express';
import { bulkJobController } from '../controllers/bulk-job.controller';

const router = Router();

// POST /bulk-jobs - Submit a bulk stage move
router.post('/', (req, res, next) => bulkJobController.createJob(req, res, next));

// GET /bulk-jobs/:id - Check job progress
router.get('/:id', (req, res, next) => bulkJobController.getJob(req, res, next));

export const bulkJobRoutes = router;
