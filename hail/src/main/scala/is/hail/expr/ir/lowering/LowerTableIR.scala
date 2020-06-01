package is.hail.expr.ir.lowering

import is.hail.expr.ir._
import is.hail.types
import is.hail.types.virtual._
import is.hail.methods.{ForceCountTable, NPartitionsTable}
import is.hail.rvd.{AbstractRVDSpec, RVDPartitioner}
import is.hail.types.TableType
import is.hail.utils._
import org.apache.spark.sql.Row

class LowererUnsupportedOperation(msg: String = null) extends Exception(msg)

case class ShuffledStage(child: TableStage)

case class Binding(name: String, value: IR)

object TableStage {
  def apply(
    globals: IR,
    partitioner: RVDPartitioner,
    contexts: IR,
    body: (Ref) => IR): TableStage = {
    val globalsID = genUID()
    TableStage(
      FastIndexedSeq(globalsID -> globals),
      Set(globalsID),
      Ref(globalsID, globals.typ),
      partitioner,
      contexts,
      body)
  }

  def apply(
    letBindings: IndexedSeq[(String, IR)],
    broadcastVals: Set[String],
    globals: IR,
    partitioner: RVDPartitioner,
    contexts: IR,
    partition: Ref => IR
  ): TableStage = {
    val ctxType = contexts.typ.asInstanceOf[TStream].elementType
    val ctxRef = Ref(genUID(), ctxType)

    new TableStage(letBindings, broadcastVals, globals, partitioner, contexts, ctxRef.name, partition(ctxRef))
  }
}

class TableStage(
  val letBindings: IndexedSeq[(String, IR)],
  val broadcastVals: Set[String],
  val globals: IR,
  val partitioner: RVDPartitioner,
  val contexts: IR,
  val ctxRefName: String,
  val partitionIR: IR) { self =>

  def ctxType: Type = contexts.typ.asInstanceOf[TStream].elementType
  def rowType: TStruct = partitionIR.typ.asInstanceOf[TStream].elementType.asInstanceOf[TStruct]
  def kType: TStruct = partitioner.kType
  def key: IndexedSeq[String] = kType.fieldNames
  def globalType: TStruct = globals.typ.asInstanceOf[TStruct]
  def tableType: TableType = TableType(rowType, key, globalType)

  assert(kType.fields.forall(f => rowType.field(f.name).typ == f.typ))

  def partition(ctx: IR): IR = {
    require(ctx.typ == ctxType)
    Let(ctxRefName, ctx, partitionIR)
  }

  require(kType.fields.forall(f => rowType.field(f.name).typ == f.typ))

  def wrapInBindings(body: IR): IR = {
    letBindings.foldRight(body) { case ((name, binding), soFar) => Let(name, binding, soFar) }
  }

  def mapPartition(f: IR => IR): TableStage =
    new TableStage(letBindings, broadcastVals, globals, partitioner, contexts, ctxRefName, f(partitionIR))

  def zipPartitions(right: TableStage, newGlobals: (IR, IR) => IR, body: (IR, IR) => IR): TableStage = {
    val left = this
    val leftCtxTyp = left.ctxType
    val rightCtxTyp = right.ctxType

    val leftCtxRef = Ref(genUID(), leftCtxTyp)
    val rightCtxRef = Ref(genUID(), rightCtxTyp)

    val leftCtxStructField = genUID()
    val rightCtxStructField = genUID()

    val zippedCtxs = StreamZip(
      FastIndexedSeq(left.contexts, right.contexts),
      FastIndexedSeq(leftCtxRef.name, rightCtxRef.name),
      MakeStruct(FastIndexedSeq(leftCtxStructField -> leftCtxRef,
                                rightCtxStructField -> rightCtxRef)),
      ArrayZipBehavior.AssertSameLength)

    TableStage(
      left.letBindings ++ right.letBindings,
      left.broadcastVals ++ right.broadcastVals,
      newGlobals(left.globals, right.globals),
      left.partitioner,
      zippedCtxs,
      ctxRef => {
        bindIR(left.partition(GetField(ctxRef, leftCtxStructField))) { lPart =>
          bindIR(right.partition(GetField(ctxRef, rightCtxStructField))) { rPart =>
            body(lPart, rPart)
          }
        }
      })
  }

  def mapContexts(f: IR => IR): TableStage = {
    val newContexts = f(contexts)
    assert(newContexts.typ == contexts.typ)

    new TableStage(letBindings, broadcastVals, globals, partitioner, newContexts, ctxRefName, partitionIR)
  }

  def mapGlobals(f: IR => IR): TableStage = {
    val newGlobals = f(globals)
    val newID = genUID()

    new TableStage(
      letBindings :+ newID -> newGlobals,
      broadcastVals + newID,
      Ref(newID, newGlobals.typ),
      partitioner,
      contexts,
      ctxRefName,
      partitionIR)
  }

  def mapCollect(f: IR => IR, bind: Boolean = true): IR = {
    val broadcastRefs = MakeStruct(letBindings.filter { case (name, _) => broadcastVals.contains(name) })
    val glob = Ref(genUID(), broadcastRefs.typ)
    val cda = CollectDistributedArray(
      contexts, broadcastRefs,
      ctxRefName, glob.name,
      broadcastVals.foldLeft(f(partitionIR))((accum, name) => Let(name, GetField(glob, name), accum)))
    if (bind) wrapInBindings(cda) else cda
  }

  def collectWithGlobals(): IR = {
    val collected = mapCollect(ToArray, bind = false)
    wrapInBindings(
      MakeStruct(FastIndexedSeq(
        "rows" -> ToArray(flatMapIR(ToStream(collected)) { elt => ToStream(elt) }),
        "global" -> globals)))
  }

  def changePartitionerNoRepartition(newPartitioner: RVDPartitioner): TableStage =
    new TableStage(letBindings, broadcastVals, globals, newPartitioner, contexts, ctxRefName, partitionIR)

  def repartitionNoShuffle(newPartitioner: RVDPartitioner): TableStage = {
    require(newPartitioner.satisfiesAllowedOverlap(newPartitioner.kType.size - 1))
    require(newPartitioner.kType.isPrefixOf(kType))

    val boundType = RVDPartitioner.intervalIRRepresentation(newPartitioner.kType)
    val partitionMapping: IndexedSeq[Row] = newPartitioner.rangeBounds.map { i =>
      Row(RVDPartitioner.intervalToIRRepresentation(i, newPartitioner.kType.size), partitioner.queryInterval(i))
    }
    val partitionMappingType = TStruct(
      "partitionBound" -> boundType,
      "parentPartitions" -> TArray(TInt32)
    )

    val prevContextUID = genUID()
    val mappingUID = genUID()
    val idxUID = genUID()
    val newContexts = Let(
      prevContextUID,
      ToArray(contexts),
      StreamMap(
        ToStream(
          Literal(
            TArray(partitionMappingType),
            partitionMapping)),
        mappingUID,
        MakeStruct(
          FastSeq(
            "partitionBound" -> GetField(Ref(mappingUID, partitionMappingType), "partitionBound"),
            "oldContexts" -> ToArray(
              StreamMap(
                ToStream(GetField(Ref(mappingUID, partitionMappingType), "parentPartitions")),
                idxUID,
                ArrayRef(Ref(prevContextUID, TArray(contexts.typ.asInstanceOf[TStream].elementType)), Ref(idxUID, TInt32))
              ))
          )
        )
      )
    )

    val intervalUID = genUID()
    val eltUID = genUID()
    val prevContextUIDPartition = genUID()

    TableStage(letBindings, broadcastVals, globals, newPartitioner, newContexts,
      ctxRef => {
        val body = self.partition(Ref(prevContextUIDPartition, self.contexts.typ.asInstanceOf[TStream].elementType))
        Let(
          intervalUID,
          GetField(ctxRef, "partitionBound"),
          StreamFilter(
            StreamFlatMap(
              ToStream(GetField(ctxRef, "oldContexts")),
              prevContextUIDPartition,
              body
            ),
            eltUID,
            invoke("partitionIntervalContains",
              TBoolean,
              Ref(intervalUID, boundType),
              SelectFields(Ref(eltUID, body.typ.asInstanceOf[TStream].elementType), newPartitioner.kType.fieldNames)
            )
          ))
      })
  }

  def changeKey(execCtx: ExecuteContext, newKey: IndexedSeq[String]): TableStage = {
    val sorted = execCtx.backend.lowerDistributedSort(execCtx, this, newKey.map(k => SortField(k, Ascending)))
    assert(sorted.kType.fieldNames.sameElements(newKey))
    sorted
  }

  def extendKeyPreservesPartitioning(newKey: IndexedSeq[String]): TableStage = {
    require(newKey startsWith kType.fieldNames)
    require(newKey.forall(rowType.fieldNames.contains))

    val newKeyType = rowType.typeAfterSelectNames(newKey)
    if (RVDPartitioner.isValid(newKeyType, partitioner.rangeBounds)) {
      changePartitionerNoRepartition(partitioner.copy(kType = newKeyType))
    } else {
      val adjustedPartitioner = partitioner.strictify
      repartitionNoShuffle(adjustedPartitioner)
        .changePartitionerNoRepartition(adjustedPartitioner.copy(kType = newKeyType))
    }
  }

  // Return TableStage whose key equals or at least starts with 'newKey'.
  def enforceKey(execCtx: ExecuteContext, newKey: IndexedSeq[String], isSorted: Boolean = false): TableStage = {
    require(newKey.forall(rowType.hasField))
    val nPreservedFields = kType.fieldNames.zip(newKey).takeWhile { case (l, r) => l == r }.length
    require(!isSorted || nPreservedFields > 0 || newKey.isEmpty)

    if (nPreservedFields == newKey.length)
      this
    else if (isSorted)
      changePartitionerNoRepartition(partitioner.coarsen(nPreservedFields))
        .extendKeyPreservesPartitioning(newKey)
//        .checkKeyOrdering()
    else
      changeKey(execCtx, newKey)
  }

  def orderedJoin(
    right: TableStage,
    joinKey: Int,
    joinType: String,
    globalJoiner: (IR, IR) => IR,
    joiner: (Ref, Ref) => IR
  ): TableStage = {
    assert(this.kType.truncate(joinKey).isIsomorphicTo(right.kType.truncate(joinKey)))

    val newPartitioner = {
      def leftPart: RVDPartitioner = this.partitioner.strictify
      def rightPart: RVDPartitioner = right.partitioner.coarsen(joinKey).extendKey(this.kType)
      (joinType: @unchecked) match {
        case "left" => leftPart
        case "right" => rightPart
        case "inner" => leftPart.intersect(rightPart)
        case "outer" => RVDPartitioner.generate(
          this.kType,
          leftPart.rangeBounds ++ rightPart.rangeBounds)
      }
    }
    val repartitionedLeft: TableStage = this.repartitionNoShuffle(newPartitioner)

    val partitionJoiner: (IR, IR) => IR = (lPart, rPart) => {
      val lEltType = lPart.typ.asInstanceOf[TStream].elementType.asInstanceOf[TStruct]
      val rEltType = rPart.typ.asInstanceOf[TStream].elementType.asInstanceOf[TStruct]

      val lKey = this.kType.fieldNames.take(joinKey)
      val rKey = right.kType.fieldNames.take(joinKey)

      val lEltRef = Ref(genUID(), lEltType)
      val rEltRef = Ref(genUID(), rEltType)

      StreamJoin(lPart, rPart, lKey, rKey, lEltRef.name, rEltRef.name, joiner(lEltRef, rEltRef), joinType)
    }

    val newKey = kType.fieldNames ++ right.kType.fieldNames.drop(joinKey)

    repartitionedLeft.alignAndZipPartitions(
      right,
      joinKey,
      repartitionedLeft.kType.size,
      globalJoiner,
      partitionJoiner)
      .extendKeyPreservesPartitioning(newKey)
  }

  // New key type must be prefix of left key type. 'joinKey' must be prefix of
  // both left key and right key. 'zipper' must take all output key values from
  // left iterator, and be monotonic on left iterator (it can drop or duplicate
  // elements of left iterator, or insert new elements in order, but cannot
  // rearrange them), and output region values must conform to 'newTyp'. The
  // partitioner of the resulting RVD will be left partitioner truncated
  // to new key. Each partition will be computed by 'zipper', with corresponding
  // partition of 'this' as first iterator, and with all rows of 'that' whose
  // 'joinKey' might match something in partition as the second iterator.
  def alignAndZipPartitions(
    right: TableStage,
    joinKey: Int,
    newKey: Int,
    globalJoiner: (IR, IR) => IR,
    joiner: (IR, IR) => IR
  ): TableStage = {
    require(newKey <= kType.size)
    require(joinKey <= kType.size)
    require(joinKey <= right.kType.size)

    val newPartitioner = partitioner.coarsen(newKey)
    val leftKeyToRightKeyMap = kType.fieldNames.zip(right.kType.fieldNames).toMap
    val newRightPartitioner = partitioner.coarsen(joinKey).strictify.rename(leftKeyToRightKeyMap)
    val repartitionedRight = right.repartitionNoShuffle(newRightPartitioner)
    zipPartitions(repartitionedRight, globalJoiner, joiner)
      .changePartitionerNoRepartition(newPartitioner)
  }
}

object LowerTableIR {
  def apply(ir: IR, typesToLower: DArrayLowering.Type, ctx: ExecuteContext): IR = {
    def lowerIR(ir: IR) = LowerIR.lower(ir, typesToLower, ctx)

    def lower(tir: TableIR): TableStage = {
      if (typesToLower == DArrayLowering.BMOnly)
        throw new LowererUnsupportedOperation("found TableIR in lowering; lowering only BlockMatrixIRs.")

      val lowered = tir match {
        case TableRead(typ, dropRows, reader) =>
          if (dropRows) {
            val globals = reader.lowerGlobals(ctx, typ.globalType)
            val globalsID = genUID()

            TableStage(
              FastIndexedSeq(globalsID -> globals),
              Set(globalsID),
              Ref(globalsID, globals.typ),
              RVDPartitioner.empty(typ.keyType),
              MakeStream(FastIndexedSeq(), TStream(TStruct.empty)),
              _ => MakeStream(FastIndexedSeq(), TStream(typ.rowType)))
          } else
            reader.lower(ctx, typ)

        case TableParallelize(rowsAndGlobal, nPartitions) =>
          val nPartitionsAdj = nPartitions.getOrElse(16)
          val loweredRowsAndGlobal = lowerIR(rowsAndGlobal)
          val loweredRowsAndGlobalRef = Ref(genUID(), loweredRowsAndGlobal.typ)

          val contextType = TStruct(
            "elements" -> TArray(GetField(loweredRowsAndGlobalRef, "rows").typ.asInstanceOf[TArray].elementType)
          )
          val numRows = ArrayLen(GetField(loweredRowsAndGlobalRef, "rows"))

          val numNonEmptyPartitions = If(numRows < nPartitionsAdj, numRows, nPartitionsAdj)
          val numNonEmptyPartitionsRef = Ref(genUID(), numNonEmptyPartitions.typ)

          val q = numRows floorDiv numNonEmptyPartitionsRef
          val qRef = Ref(genUID(), q.typ)

          val remainder = numRows - qRef * numNonEmptyPartitionsRef
          val remainderRef = Ref(genUID(), remainder.typ)

          val context = MakeStream((0 until nPartitionsAdj).map { partIdx =>
            val length = (numRows - partIdx + nPartitionsAdj - 1) floorDiv nPartitionsAdj

            val start = If(numNonEmptyPartitionsRef >= partIdx,
              If(remainderRef > 0,
                If(remainderRef < partIdx, qRef * partIdx + remainderRef, (qRef + 1) * partIdx),
                qRef * partIdx
              ),
              0
            )

            val elements = bindIR(start) { startRef =>
              ToArray(mapIR(rangeIR(startRef, startRef + length)) { elt =>
                ArrayRef(GetField(loweredRowsAndGlobalRef, "rows"), elt)
              })
            }
            MakeStruct(FastIndexedSeq("elements" -> elements))
          }, TStream(contextType))

          val globalsIR = GetField(loweredRowsAndGlobalRef, "global")
          val globalsRef = Ref(genUID(), globalsIR.typ)

          TableStage(
            FastIndexedSeq[(String, IR)](
              (loweredRowsAndGlobalRef.name, loweredRowsAndGlobal),
              (globalsRef.name, globalsIR),
              (numNonEmptyPartitionsRef.name, numNonEmptyPartitions),
              (qRef.name, q),
              (remainderRef.name, remainder)
            ),
            Set(globalsRef.name),
            globalsRef,
            RVDPartitioner.unkeyed(nPartitionsAdj),
            context,
            ctxRef => ToStream(GetField(ctxRef, "elements")))

        case TableRange(n, nPartitions) =>
          val nPartitionsAdj = math.max(math.min(n, nPartitions), 1)
          val partCounts = partition(n, nPartitionsAdj)
          val partStarts = partCounts.scanLeft(0)(_ + _)

          val contextType = TStruct(
            "start" -> TInt32,
            "end" -> TInt32)

          val i = Ref(genUID(), TInt32)
          val ranges = Array.tabulate(nPartitionsAdj) { i => partStarts(i) -> partStarts(i + 1) }

          TableStage(
            FastIndexedSeq.empty[(String, IR)],
            Set(),
            MakeStruct(FastSeq()),
            new RVDPartitioner(Array("idx"), tir.typ.rowType,
              ranges.map { case (start, end) =>
                Interval(Row(start), Row(end), includesStart = true, includesEnd = false)
              }),
            MakeStream(ranges.map { case (start, end) =>
              MakeStruct(FastIndexedSeq("start" -> start, "end" -> end)) },
              TStream(contextType)),
            ctxRef => mapIR(rangeIR(GetField(ctxRef, "start"), GetField(ctxRef, "end"))) { i =>
              MakeStruct(FastSeq("idx" -> i))
            })

        case TableMapGlobals(child, newGlobals) =>
          lower(child).mapGlobals(old => Let("global", old, newGlobals))

        case x@TableAggregateByKey(child, expr) =>
          val loweredChild = lower(child)

          loweredChild.repartitionNoShuffle(loweredChild.partitioner.coarsen(child.typ.key.length).strictify)
            .mapPartition { partition =>
              val sUID = genUID()

              StreamMap(
                StreamGroupByKey(partition, child.typ.key),
                sUID,
                StreamAgg(
                  Ref(sUID, TStream(child.typ.rowType)),
                  "row",
                  bindIRs(ArrayRef(ApplyAggOp(FastSeq(I32(1)), FastSeq(SelectFields(Ref("row", child.typ.rowType), child.typ.key)),
                    AggSignature(Take(), FastSeq(TInt32), FastSeq(child.typ.keyType))), I32(0)), // FIXME: would prefer a First() agg op
                    expr) { case Seq(key, value) =>
                    MakeStruct(child.typ.key.map(k => (k, GetField(key, k))) ++ expr.typ.asInstanceOf[TStruct].fieldNames.map { f =>
                      (f, GetField(value, f))
                    })
                  }
                )
              )
            }

        case TableDistinct(child) =>
          val loweredChild = lower(child)

          loweredChild.repartitionNoShuffle(loweredChild.partitioner.coarsen(child.typ.key.length).strictify)
            .mapPartition { partition =>
              flatMapIR(StreamGroupByKey(partition, child.typ.key)) { groupRef =>
                StreamTake(groupRef, 1)
              }
            }

        case TableFilter(child, cond) =>
          val row = Ref(genUID(), child.typ.rowType)
          val loweredChild = lower(child)
          val env: Env[IR] = Env("row" -> row, "global" -> loweredChild.globals)
          loweredChild.mapPartition(rows => StreamFilter(rows, row.name, Subst(cond, BindingEnv(env))))

        case TableHead(child, targetNumRows) =>
          val loweredChild = lower(child)

          val partitionSizeArrayFunc = genUID()
          val howManyPartsToTry = Ref(genUID(), TInt32)
          val childContexts = Ref(genUID(), TArray(coerce[TStream](loweredChild.contexts.typ).elementType))
          val partitionSizeArray = TailLoop(
            partitionSizeArrayFunc,
            FastIndexedSeq(howManyPartsToTry.name -> 4),
            bindIR(loweredChild.mapContexts(_ => StreamTake(ToStream(childContexts), howManyPartsToTry))
                               .mapCollect(StreamLen)) { counts =>
              If((Cast(streamSumIR(ToStream(counts)), TInt64) >= targetNumRows) || (ArrayLen(childContexts) <= ArrayLen(counts)),
                counts,
                Recur(partitionSizeArrayFunc, FastIndexedSeq(howManyPartsToTry * 4), TArray(TInt32)))
            })
          val partitionSizeArrayRef = Ref(genUID(), partitionSizeArray.typ)

          val answerTuple = bindIR(ArrayLen(partitionSizeArrayRef)) { numPartitions =>
            val howManyPartsToKeep = genUID()
            val i = Ref(genUID(), TInt32)
            val numLeft = Ref(genUID(), TInt64)
            def makeAnswer(howManyParts: IR, howManyFromLast: IR) = MakeTuple(FastIndexedSeq((0, howManyParts), (1, howManyFromLast)))

            If(numPartitions ceq 0,
              makeAnswer(0, 0L),
              TailLoop(howManyPartsToKeep, FastIndexedSeq(i.name -> 0, numLeft.name -> targetNumRows),
                If((i ceq numPartitions - 1) || ((numLeft - Cast(ArrayRef(partitionSizeArrayRef, i), TInt64)) <= 0L),
                  makeAnswer(i + 1, numLeft),
                  Recur(howManyPartsToKeep, FastIndexedSeq(i + 1, numLeft - Cast(ArrayRef(partitionSizeArrayRef, i), TInt64)), TTuple(TInt32, TInt64))
                )
              )
            )
          }

          val newCtxs = bindIR(answerTuple) { answerTupleRef =>
            val numParts = GetTupleElement(answerTupleRef, 0)
            val numElementsFromLastPart = GetTupleElement(answerTupleRef, 1)
            val onlyNeededPartitions = StreamTake(ToStream(childContexts), numParts)
            val howManyFromEachPart = mapIR(rangeIR(numParts)) { idxRef =>
              If(idxRef ceq (numParts - 1),
                 Cast(numElementsFromLastPart, TInt32),
                 ArrayRef(partitionSizeArrayRef, idxRef))
            }
            StreamZip(
              FastIndexedSeq(onlyNeededPartitions, howManyFromEachPart),
              FastIndexedSeq("part", "howMany"),
              MakeStruct(FastSeq("numberToTake" -> Ref("howMany", TInt32),
                                 "old" -> Ref("part", loweredChild.ctxType))),
              ArrayZipBehavior.AssumeSameLength)
          }

          TableStage(
            loweredChild.letBindings
              :+ childContexts.name -> ToArray(loweredChild.contexts)
              :+ partitionSizeArrayRef.name -> partitionSizeArray,
            loweredChild.broadcastVals,
            loweredChild.globals,
            loweredChild.partitioner,
            newCtxs,
            ctxRef => bindIR(GetField(ctxRef, "old")) { oldRef =>
              StreamTake(loweredChild.partition(oldRef), GetField(ctxRef, "numberToTake"))
            })

        case TableTail(child, targetNumRows) =>
          val loweredChild = lower(child)

          val partitionSizeArrayFunc = genUID()
          val howManyPartsToTry = Ref(genUID(), TInt32)
          val childContexts = Ref(genUID(), TArray(coerce[TStream](loweredChild.contexts.typ).elementType))

          val totalNumPartitions = Ref(genUID(), TInt32)
          val partitionSizeArray =
            TailLoop(partitionSizeArrayFunc, FastIndexedSeq(howManyPartsToTry.name -> 4),
              bindIR(loweredChild.mapContexts(_ => StreamDrop(ToStream(childContexts), maxIR(totalNumPartitions - howManyPartsToTry, 0)))
                                 .mapCollect(StreamLen)) { counts =>
                If((Cast(streamSumIR(ToStream(counts)), TInt64) >= targetNumRows) || (totalNumPartitions <= ArrayLen(counts)),
                  counts,
                  Recur(partitionSizeArrayFunc, FastIndexedSeq(howManyPartsToTry * 4), TArray(TInt32))
                )
              }
            )

          val partitionSizeArrayRef = Ref(genUID(), partitionSizeArray.typ)

          // First element is how many partitions to drop from partitionSizeArrayRef, second is how many to keep from first kept element.
          val answerTuple = bindIR(ArrayLen(partitionSizeArrayRef)) { numPartitions =>
            val howManyPartsToDrop = genUID()
            val i = Ref(genUID(), TInt32)
            val numLeft = Ref(genUID(), TInt64)
            def makeAnswer(howManyParts: IR, howManyFromLast: IR) = MakeTuple(FastIndexedSeq((0, howManyParts), (1, howManyFromLast)))

            If(numPartitions ceq 0,
              makeAnswer(0, 0L),
              TailLoop(howManyPartsToDrop, FastIndexedSeq(i.name -> numPartitions, numLeft.name -> targetNumRows),
                If((i ceq 1) || ((numLeft - Cast(ArrayRef(partitionSizeArrayRef, i - 1), TInt64)) <= 0L),
                  makeAnswer(i - 1, numLeft),
                  Recur(howManyPartsToDrop, FastIndexedSeq(i - 1, numLeft - Cast(ArrayRef(partitionSizeArrayRef, i - 1), TInt64)), TTuple(TInt32, TInt64))
                )
              )
            )
          }

          val newCtxs = bindIR(answerTuple) { answerTupleRef =>
            val numPartsToDropFromPartitionSizeArray = GetTupleElement(answerTupleRef, 0)
            val numElementsFromFirstPart = GetTupleElement(answerTupleRef, 1)
            val numPartsToDropFromTotal = numPartsToDropFromPartitionSizeArray + (totalNumPartitions - ArrayLen(partitionSizeArrayRef))
            val onlyNeededPartitions = StreamDrop(ToStream(childContexts), numPartsToDropFromTotal)
            val howManyFromEachPart = mapIR(rangeIR(StreamLen(onlyNeededPartitions))) { idxRef =>
              If(idxRef ceq 0,
                 Cast(numElementsFromFirstPart, TInt32),
                 ArrayRef(partitionSizeArrayRef, idxRef))
            }
            StreamZip(
              FastIndexedSeq(onlyNeededPartitions,
                             howManyFromEachPart,
                             StreamDrop(ToStream(partitionSizeArrayRef), numPartsToDropFromPartitionSizeArray)),
              FastIndexedSeq("part", "howMany", "partLength"),
              MakeStruct(FastIndexedSeq(
                "numberToDrop" -> maxIR(0, Ref("partLength", TInt32) - Ref("howMany", TInt32)),
                "old" -> Ref("part", loweredChild.ctxType))),
              ArrayZipBehavior.AssertSameLength)
          }

          TableStage(
            loweredChild.letBindings ++ FastIndexedSeq(
              childContexts.name -> ToArray(loweredChild.contexts),
              totalNumPartitions.name -> ArrayLen(childContexts),
              partitionSizeArrayRef.name -> partitionSizeArray
            ),
            loweredChild.broadcastVals,
            loweredChild.globals,
            loweredChild.partitioner,
            newCtxs,
            ctxRef => bindIR(GetField(ctxRef, "old")) { oldRef =>
              StreamDrop(loweredChild.partition(oldRef), GetField(ctxRef, "numberToDrop"))
            })

        case TableMapRows(child, newRow) =>
          if (ContainsScan(newRow))
            throw new LowererUnsupportedOperation(s"scans are not supported: \n${ Pretty(newRow) }")
          val loweredChild = lower(child)
          loweredChild.mapPartition(rows => mapIR(rows) { row =>
            val env: Env[IR] = Env("row" -> row, "global" -> loweredChild.globals)
            Subst(newRow, BindingEnv(env, scan = Some(env)))
          })

        case TableGroupWithinPartitions(child, groupedStructName, n) =>
          val loweredChild = lower(child)
          val keyFields = FastIndexedSeq(child.typ.keyType.fieldNames: _*)
          loweredChild.mapPartition { part =>
            val grouped =  StreamGrouped(part, n)
            val groupedArrays = mapIR(grouped) (group => ToArray(group))
            val withKeys = mapIR(groupedArrays) {group =>
              bindIR(group) { groupRef =>
                bindIR(ArrayRef(groupRef, 0)) { firstElement =>
                  val firstElementKeys = keyFields.map(keyField => (keyField, GetField(firstElement, keyField)))
                  val rowStructFields = firstElementKeys ++ FastIndexedSeq(groupedStructName -> groupRef)
                  MakeStruct(rowStructFields)
                }
              }
            }
            withKeys
          }

        case TableKeyBy(child, newKey, isSorted: Boolean) =>
          val loweredChild = lower(child)
          loweredChild.enforceKey(ctx, newKey, isSorted)

        case TableLeftJoinRightDistinct(left, right, root) =>
          val commonKeyLength = right.typ.keyType.size
          val loweredLeft = lower(left)
          val leftKeyToRightKeyMap = left.typ.keyType.fieldNames.zip(right.typ.keyType.fieldNames).toMap
          val newRightPartitioner = loweredLeft.partitioner.coarsen(commonKeyLength)
            .rename(leftKeyToRightKeyMap)
          val loweredRight = lower(right).repartitionNoShuffle(newRightPartitioner)

          loweredLeft.zipPartitions(
            loweredRight,
            (lGlobals, _) => lGlobals,
            (leftPart, rightPart) => {
            val leftElementRef = Ref(genUID(), left.typ.rowType)
            val rightElementRef = Ref(genUID(), right.typ.rowType)

            val (typeOfRootStruct, _) = right.typ.rowType.filterSet(right.typ.key.toSet, false)
            val rootStruct = SelectFields(rightElementRef, typeOfRootStruct.fieldNames.toIndexedSeq)
            val joiningOp = InsertFields(leftElementRef, Seq(root -> rootStruct))
            StreamJoinRightDistinct(
              leftPart, rightPart,
              left.typ.key.take(commonKeyLength), right.typ.key,
              leftElementRef.name, rightElementRef.name,
              joiningOp, "left")
          })

        case TableJoin(left, right, joinType, joinKey) if joinType != "zip" =>
          val loweredLeft = lower(left)
          val loweredRight = lower(right)

          println(s"\nleft partitioner: ${loweredLeft.partitioner}\nright partitioner: ${loweredRight.partitioner}")

          val lKeyFields = left.typ.key.take(joinKey)
          val lValueFields = left.typ.rowType.fieldNames.filter(f => !lKeyFields.contains(f))
          val rKeyFields = right.typ.key.take(joinKey)
          val rValueFields = right.typ.rowType.fieldNames.filter(f => !rKeyFields.contains(f))

          loweredLeft.orderedJoin(
            loweredRight, joinKey, joinType,
            (lGlobals, rGlobals) => {
              val rGlobalType = rGlobals.typ.asInstanceOf[TStruct]
              val rGlobalRef = Ref(genUID(), rGlobalType)
              Let(rGlobalRef.name, rGlobals,
                  InsertFields(lGlobals, rGlobalType.fieldNames.map(f => f -> GetField(rGlobalRef, f))))
            },
            (lEltRef, rEltRef) => {
            MakeStruct(
              (lKeyFields, rKeyFields).zipped.map { (lKey, rKey) =>
                lKey -> Coalesce(FastSeq(GetField(lEltRef, lKey), GetField(rEltRef, rKey)))
              }
                ++ lValueFields.map(f => f -> GetField(lEltRef, f))
                ++ rValueFields.map(f => f -> GetField(rEltRef, f)))
          })

        case TableOrderBy(child, sortFields) =>
          val loweredChild = lower(child)
          if (TableOrderBy.isAlreadyOrdered(sortFields, loweredChild.partitioner.kType.fieldNames))
            loweredChild
          else
            ctx.backend.lowerDistributedSort(ctx, loweredChild, sortFields)

        case TableExplode(child, path) =>
          lower(child).mapPartition { rows =>
            flatMapIR(rows) { row: Ref =>
              val refs = Array.fill[Ref](path.length + 1)(null)
              val roots = Array.fill[IR](path.length)(null)
              var i = 0
              refs(0) = row
              while (i < path.length) {
                roots(i) = GetField(refs(i), path(i))
                refs(i + 1) = Ref(genUID(), roots(i).typ)
                i += 1
              }
              refs.tail.zip(roots).foldRight(
                mapIR(refs.last) { elt =>
                  path.zip(refs.init).foldRight[IR](elt) { case ((p, ref), inserted) =>
                    InsertFields(ref, FastSeq(p -> inserted))
                  }
                }) { case ((ref, root), accum) =>  Let(ref.name, root, accum) }
            }
          }

        case TableRename(child, rowMap, globalMap) =>
          val loweredChild = lower(child)
          val newGlobId = genUID()
          val newGlobals = CastRename(loweredChild.globals, loweredChild.globals.typ.asInstanceOf[TStruct].rename(globalMap))

          TableStage(
            loweredChild.letBindings :+ newGlobId -> newGlobals,
            loweredChild.broadcastVals + newGlobId,
            Ref(newGlobId, newGlobals.typ),
            loweredChild.partitioner.copy(kType = loweredChild.kType.rename(rowMap)),
            loweredChild.contexts,
            ctxRef => mapIR(loweredChild.partition(ctxRef)) { row =>
              CastRename(row, row.typ.asInstanceOf[TStruct].rename(rowMap))
            })

        case node =>
          throw new LowererUnsupportedOperation(s"undefined: \n${ Pretty(node) }")
      }

      assert(tir.typ.globalType == lowered.globalType, s"\n  ir global: ${tir.typ.globalType}\n  lowered global: ${lowered.globalType}")
      assert(tir.typ.rowType == lowered.rowType, s"\n  ir row: ${tir.typ.rowType}\n  lowered row: ${lowered.rowType}")
      assert(lowered.key startsWith tir.typ.keyType.fieldNames, s"\n  ir key: ${tir.typ.keyType.fieldNames}\n  lowered key: ${lowered.key}")

      lowered
    }

    ir match {
      case TableCount(tableIR) =>
        invoke("sum", TInt64,
          lower(tableIR).mapCollect(rows => Cast(StreamLen(rows), TInt64)))

      case TableToValueApply(child, ForceCountTable()) =>
        invoke("sum", TInt64,
          lower(child).mapCollect(rows => Cast(StreamLen(mapIR(rows)(row => Consume(row))), TInt64)))

      case TableGetGlobals(child) =>
        val stage = lower(child)
        stage.wrapInBindings(stage.globals)

      case TableCollect(child) =>
        lower(child).collectWithGlobals()

      case TableToValueApply(child, NPartitionsTable()) =>
        val lowered = lower(child)
        StreamLen(lowered.contexts)

      case node if node.children.exists(_.isInstanceOf[TableIR]) =>
        throw new LowererUnsupportedOperation(s"IR nodes with TableIR children must be defined explicitly: \n${ Pretty(node) }")
    }
  }
}
