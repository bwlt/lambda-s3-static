import path from 'path'
import * as AWS from 'aws-sdk'
import { APIGatewayProxyEvent } from 'aws-lambda'
import {
  TaskEither,
  taskify,
  fromEither,
  fromIO,
  fromLeft,
  taskEither,
} from 'fp-ts/lib/TaskEither'
import { fromNullable, Option } from 'fp-ts/lib/Option'
import { fromOption, Either } from 'fp-ts/lib/Either'
import { Task } from 'fp-ts/lib/Task'
import * as C from 'fp-ts/lib/Console'
import { compose, identity } from 'fp-ts/lib/function'
import { IO } from 'fp-ts/lib/IO'

type Response = {
  headers?: { [name: string]: string }
  statusCode: number
  body: string
}

type AWSError = {
  type: 'AWSError'
  error: AWS.AWSError
}

type InvocationError = {
  type: 'InvocationError'
  message: string
}

type EffError = AWSError | InvocationError

const awsError: (error: AWS.AWSError) => AWSError = error => ({
  type: 'AWSError',
  error,
})
const invocationError: (message: string) => InvocationError = message => ({
  type: 'InvocationError',
  message,
})

const s3 = new AWS.S3()

const jsonResponse: (
  params: { headers?: Response['headers']; body: object; statusCode: number }
) => Response = ({ headers, body, statusCode }) => ({
  headers: { ...headers, 'Content-Type': 'application/json' },
  body: JSON.stringify(body),
  statusCode,
})

const notFound = jsonResponse({
  statusCode: 404,
  body: { message: 'Not Found' },
})

const internalLambdaErrorResponse = jsonResponse({
  statusCode: 500,
  body: { message: 'Internal lambda error' },
})

const s3GetObject: (
  params: AWS.S3.GetObjectRequest
) => TaskEither<AWS.AWSError, AWS.S3.GetObjectOutput> = taskify(
  s3.getObject.bind(s3)
)

const oBucket = fromNullable(process.env.BUCKET)
const eBucket: Either<EffError, string> = fromOption(
  invocationError('Missing BUCKET env')
)(oBucket)

const matchEffError = (effError: EffError) => <R>(matcher: {
  whenAccessDenied: (awsError: AWS.AWSError) => R
  genericAWSError: (awsError: AWS.AWSError) => R
  genericInvocationError: (message: string) => R
}) => {
  switch (effError.type) {
    case 'AWSError': {
      const awsError = effError.error
      switch (awsError.code) {
        case 'AccessDenied':
          return matcher.whenAccessDenied(awsError)
        default:
          return matcher.genericAWSError(awsError)
      }
    }
    case 'InvocationError':
      return matcher.genericInvocationError(effError.message)
  }
}

const getObjectEffect: (
  key: string
) => TaskEither<EffError, AWS.S3.GetObjectOutput> = key => {
  const key1 = key
  const key2 = path.join(key, 'index.html') // key to try when key 1 is not found
  const getObject = (key: string) =>
    fromEither(eBucket).chain(bucket =>
      s3GetObject({ Key: key, Bucket: bucket }).mapLeft(awsError)
    )
  const getKey2ifKey1IsDirectory: (
    ma: TaskEither<EffError, AWS.S3.GetObjectOutput>
  ) => TaskEither<EffError, AWS.S3.GetObjectOutput> = ma =>
    ma.chain(a => {
      switch (a.ContentType) {
        case 'application/x-directory':
          return getObject(key2)
        default:
          return taskEither.of(a)
      }
    })
  const getKey2IfKey1IsNotFound: (
    ma: TaskEither<EffError, AWS.S3.GetObjectOutput>
  ) => TaskEither<EffError, AWS.S3.GetObjectOutput> = ma =>
    ma.orElse(l =>
      matchEffError(l)({
        whenAccessDenied: () => getObject(key2),
        genericAWSError: () => fromLeft(l),
        genericInvocationError: () => fromLeft(l),
      })
    )
  const f = compose(
    getKey2IfKey1IsNotFound,
    getKey2ifKey1IsDirectory,
    getObject
  )
  return f(key1)
}

const mapLambdaEventToBucketKey = (event: APIGatewayProxyEvent): string =>
  event.path.slice(1)

const create200Response = (body: string) => (contentType: string) => (
  etag: string
): Response => ({
  body,
  statusCode: 200,
  headers: {
    'Content-Type': contentType,
    ETag: etag,
  },
})

const mapGetObjectOutputToResponse: (
  getObjectOutput: AWS.S3.GetObjectOutput
) => Option<Response> = getObjectOutput => {
  const oBody = fromNullable(getObjectOutput.Body).map(b => b.toString())
  const oContentType = fromNullable(getObjectOutput.ContentType)
  const oETag = fromNullable(getObjectOutput.ETag)
  const oResponse = oETag.ap(oContentType.ap(oBody.map(create200Response)))
  return oResponse
}

const getObjectOutputToResponse: (
  ma: TaskEither<EffError, AWS.S3.GetObjectOutput>
) => TaskEither<EffError, Response> = ma =>
  ma.chain(getObjectOutput =>
    fromEither(
      fromOption(invocationError('Unexpected getObjectOutput'))(
        mapGetObjectOutputToResponse(getObjectOutput)
      )
    )
  )

const handleAccessDeniend = (
  ma: TaskEither<EffError, Response>
): TaskEither<EffError, Response> =>
  ma.orElse(effError => {
    const of: (response: Response) => TaskEither<EffError, Response> =
      taskEither.of
    const leftError: TaskEither<EffError, Response> = fromLeft(effError)
    return matchEffError(effError)({
      whenAccessDenied: () => of(notFound),
      genericAWSError: () => leftError,
      genericInvocationError: () => leftError,
    })
  })

const logError = (
  ma: TaskEither<EffError, Response>
): TaskEither<EffError, Response> => {
  const fromIO_: <A>(ma: IO<A>) => TaskEither<EffError, A> = fromIO
  return ma.orElse(effError =>
    fromIO_(C.error(effError)).chain(() => fromLeft(effError))
  )
}

const handleLeft = (ma: TaskEither<EffError, Response>): Task<Response> =>
  ma.fold(() => internalLambdaErrorResponse, identity)

const unsafeRunTask: (ma: Task<Response>) => Promise<Response> = ma => ma.run()

const handler: (event: APIGatewayProxyEvent) => Promise<Response> = compose(
  unsafeRunTask,
  handleLeft,
  logError,
  handleAccessDeniend,
  getObjectOutputToResponse,
  getObjectEffect,
  mapLambdaEventToBucketKey
)

export { handler }
