import { Router } from 'express';
import { opportunityController } from '../controllers/opportunity.controller';

const router = Router();

// POST /opportunities - Create an opportunity
router.post('/', (req, res, next) => opportunityController.create(req, res, next));

// PATCH /opportunities/:id/stage - Move single opportunity stage
router.patch('/:id/stage', (req, res, next) => opportunityController.moveStage(req, res, next));

// GET /opportunities - List opportunities in a stage with pagination
router.get('/', (req, res, next) => opportunityController.list(req, res, next));

export const opportunityRoutes = router;
